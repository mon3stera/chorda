//! The `#[plugin]` attribute macro: axum-style service extraction for
//! chorda plugin `apply` methods.
//!
//! A plugin declares its dependencies in `inject`, then fetches the same
//! services again inside `apply` and writes a dead `let-else` for each —
//! the same knowledge three times. The macro collapses that: service
//! parameters written after `ctx` are extracted from the context. An
//! `Arc<T>` parameter is a hard dependency, derived into `inject` so the
//! kernel holds the plugin until `T` resolves; an `Option<Arc<T>>`
//! parameter is a soft dependency, never gating startup and reading `None`
//! when the service is absent.
//!
//! ```ignore
//! #[chorda::plugin]
//! impl Plugin for McpPlugin {
//!     async fn apply(&self, ctx: Ctx, config: Arc<Config>, db: Option<Arc<Db>>) -> anyhow::Result<()> {
//!         // config is present by construction: inject was derived from it.
//!
//!         // db is None when no Db service exists — the plugin still starts.
//!         Ok(())
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    AngleBracketedGenericArguments, FnArg, GenericArgument, ImplItem, ItemImpl, PathArguments,
    PathSegment, ReturnType, Signature, Type, TypePath, parse_macro_input,
};

/// Transforms a plugin's `apply` into the trait-conforming signature and
/// derives `inject` from its service parameters.
///
/// The input must be an `impl Plugin for Type` block containing `async fn
/// apply(&self, ctx: Ctx, ...)`. Every parameter after `ctx` is a service
/// parameter:
///
/// - `Arc<T>` — a hard dependency: extracted, and added to a derived
///   `inject` so the kernel holds the plugin until `T` resolves;
/// - `Option<Arc<T>>` — a soft dependency: extracted as `None` when absent,
///   and never added to `inject`.
///
/// A hand-written `inject` in the same block wins over the derived one.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(Span::call_site(), "#[plugin] takes no arguments")
            .to_compile_error()
            .into();
    }

    let impl_block = parse_macro_input!(item as ItemImpl);

    expand(impl_block)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// One service parameter of the user's `apply`.
struct ServiceParam {
    /// The parameter pattern, passed through verbatim (`config`, `mut db`).
    pat: syn::Ident,
    /// The parameter type: `Arc<T>` (hard) or `Option<Arc<T>>` (soft).
    ty: Type,
    /// The service type `T` — what the derived `inject` names.
    dependency: Type,
    /// Whether the parameter is a soft dependency (`Option<Arc<T>>`).
    soft: bool,
}

/// The pieces taken from the user's `apply`.
struct UserApply {
    signature: Signature,
    attrs: Vec<syn::Attribute>,
    body: proc_macro2::TokenStream,
}

fn expand(impl_block: ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
    let is_plugin_impl = impl_block
        .trait_
        .as_ref()
        .is_some_and(|(_, path, _)| path.segments.last().is_some_and(|s| s.ident == "Plugin"));

    if !is_plugin_impl {
        return Err(syn::Error::new_spanned(
            &impl_block,
            "#[plugin] must annotate an `impl Plugin for Type` block",
        ));
    }

    let impl_span = impl_block.span();
    let items = impl_block.items;

    let mut remaining: Vec<ImplItem> = Vec::new();
    let mut user_apply: Option<UserApply> = None;

    for item in items {
        match item {
            ImplItem::Fn(method) if method.sig.ident == "apply" => {
                if user_apply.is_some() {
                    return Err(syn::Error::new_spanned(
                        &method.sig,
                        "duplicate `apply` in a #[plugin] impl",
                    ));
                }

                let body = &method.block;

                user_apply = Some(UserApply {
                    signature: method.sig.clone(),
                    attrs: method.attrs.clone(),
                    body: quote! { #body },
                });
            }
            other => remaining.push(other),
        }
    }

    let UserApply {
        signature,
        attrs,
        body,
    } = user_apply.ok_or_else(|| {
        syn::Error::new(
            impl_span,
            "#[plugin] requires an `apply` method in the impl",
        )
    })?;

    let services = split_apply_inputs(&signature)?;
    let user_inject = remaining
        .iter()
        .any(|item| matches!(item, ImplItem::Fn(method) if method.sig.ident == "inject"));

    let (impl_generics, type_generics, where_clause) = impl_block.generics.split_for_impl();
    let self_ty = &impl_block.self_ty;

    let inherent_name = quote::format_ident!("__chorda_plugin_apply");

    let mut extracted = Vec::new();

    for service in &services {
        let ty = &service.ty;
        let pat = &service.pat;

        extracted.push(quote_spanned! { ty.span() =>
            let #pat = <#ty as ::chorda::FromService>::from_service(&ctx)?;
        });
    }

    let signature_params: Vec<proc_macro2::TokenStream> = services
        .iter()
        .map(|service| {
            let ty = &service.ty;
            let pat = &service.pat;

            quote! { #pat: #ty }
        })
        .collect();

    let call_args: Vec<&syn::Ident> = services.iter().map(|service| &service.pat).collect();

    let inject: Option<proc_macro2::TokenStream> = if user_inject {
        None
    } else if !services.is_empty() {
        let keys = services.iter().map(|service| {
            let dependency = &service.dependency;

            if service.soft {
                quote! { ::chorda::Dependency::soft(::chorda::ServiceKey::of::<#dependency>()) }
            } else {
                quote! { ::chorda::Dependency::hard(::chorda::ServiceKey::of::<#dependency>()) }
            }
        });

        Some(quote! {
            fn inject(&self) -> Vec<::chorda::Dependency> {
                vec![#(#keys),*]
            }
        })
    } else {
        None
    };

    let return_type = match &signature.output {
        ReturnType::Type(_, ty) => quote! { -> #ty },
        ReturnType::Default => quote! {},
    };

    Ok(quote! {
        #[::chorda::async_trait]
        impl #impl_generics ::chorda::Plugin for #self_ty #type_generics #where_clause {
            #(#remaining)*

            #inject

            async fn apply(&self, ctx: ::chorda::Ctx) #return_type {
                #(#extracted)*

                self.#inherent_name(ctx, #(#call_args),*).await
            }
        }

        impl #impl_generics #self_ty #type_generics #where_clause {
            #(#attrs)*
            async fn #inherent_name(&self, ctx: ::chorda::Ctx, #(#signature_params),*) #return_type #body
        }
    })
}

/// Splits the user's `apply` inputs into the receiver, the `ctx` pattern,
/// and the service parameters; validates each service type's shape.
fn split_apply_inputs(signature: &Signature) -> syn::Result<Vec<ServiceParam>> {
    let mut inputs = signature.inputs.iter().cloned();

    match inputs.next() {
        Some(FnArg::Receiver(receiver))
            if receiver.reference.is_some() && receiver.mutability.is_none() => {}
        Some(FnArg::Receiver(receiver)) => {
            return Err(syn::Error::new_spanned(
                receiver,
                "#[plugin] `apply` must take `&self`",
            ));
        }
        Some(other) => {
            return Err(syn::Error::new_spanned(
                other,
                "#[plugin] `apply` must start with `&self`",
            ));
        }
        None => {
            return Err(syn::Error::new_spanned(
                signature,
                "#[plugin] `apply` must take `&self`",
            ));
        }
    }

    match inputs.next() {
        Some(FnArg::Typed(arg)) if ends_with(&arg.ty, "Ctx") => {}
        Some(FnArg::Typed(arg)) => {
            return Err(syn::Error::new_spanned(
                arg,
                "#[plugin] `apply` must take `ctx: Ctx` after `&self`",
            ));
        }
        Some(receiver) => {
            return Err(syn::Error::new_spanned(
                receiver,
                "#[plugin] `apply` must take `ctx: Ctx` after `&self`",
            ));
        }
        None => {
            return Err(syn::Error::new_spanned(
                signature,
                "#[plugin] `apply` must take `ctx: Ctx` after `&self`",
            ));
        }
    };

    let mut services = Vec::new();

    for input in inputs {
        let syn::PatType { pat, ty, .. } = match input {
            FnArg::Typed(arg) => arg,
            FnArg::Receiver(receiver) => {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "only one `self` receiver is allowed",
                ));
            }
        };

        let pat = pattern_ident(&pat)?.clone();
        let (dependency, soft, ty) = classify_service(&ty)?;

        services.push(ServiceParam {
            pat,
            ty,
            dependency,
            soft,
        });
    }

    Ok(services)
}

/// `Arc<T>` is a hard dependency; `Option<Arc<T>>` is soft. Anything else is
/// an error — services are stored as `Arc` and cannot be cloned out.
fn classify_service(ty: &Type) -> syn::Result<(Type, bool, Type)> {
    let Some((name, arguments)) = unwrap_segment(ty) else {
        return Err(syn::Error::new_spanned(
            ty,
            "service parameters must be `Arc<T>` or `Option<Arc<T>>`",
        ));
    };

    if name == "Arc" {
        let inner = single_type_argument(arguments).ok_or_else(|| {
            syn::Error::new_spanned(ty, "`Arc` service parameters take one type argument")
        })?;

        return Ok((inner.clone(), false, ty.clone()));
    }

    if name == "Option" {
        let inner = single_type_argument(arguments).ok_or_else(|| {
            syn::Error::new_spanned(ty, "`Option` service parameters take one type argument")
        })?;

        let (inner_name, inner_arguments) = unwrap_segment(inner).ok_or_else(|| {
            syn::Error::new_spanned(ty, "soft dependencies must be `Option<Arc<T>>`")
        })?;

        if inner_name != "Arc" {
            return Err(syn::Error::new_spanned(
                ty,
                "soft dependencies must be `Option<Arc<T>>`",
            ));
        }

        let dependency = single_type_argument(inner_arguments).ok_or_else(|| {
            syn::Error::new_spanned(ty, "`Arc` service parameters take one type argument")
        })?;

        return Ok((dependency.clone(), true, ty.clone()));
    }

    Err(syn::Error::new_spanned(
        ty,
        "service parameters must be `Arc<T>` or `Option<Arc<T>>`",
    ))
}

fn unwrap_segment(ty: &Type) -> Option<(&syn::Ident, &PathArguments)> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };

    let segment: &PathSegment = path.segments.last()?;

    Some((&segment.ident, &segment.arguments))
}

fn single_type_argument(arguments: &PathArguments) -> Option<&Type> {
    let AngleBracketedGenericArguments { args, .. } = match arguments {
        PathArguments::AngleBracketed(arguments) => arguments,
        _ => return None,
    };

    match args.first()? {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

fn pattern_ident(pat: &syn::Pat) -> syn::Result<&syn::Ident> {
    match pat {
        syn::Pat::Ident(syn::PatIdent { ident, .. }) => Ok(ident),
        other => Err(syn::Error::new_spanned(
            other,
            "service parameters must be plain identifiers",
        )),
    }
}

fn ends_with(ty: &Type, name: &str) -> bool {
    unwrap_segment(ty).is_some_and(|(ident, _)| ident == name)
}
