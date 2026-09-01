use crate::service::ServiceKey;

pub trait Plugin {
    fn deps() -> &'static [ServiceKey];
}