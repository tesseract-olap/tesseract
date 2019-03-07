mod detection;
pub mod shared;

pub use self::detection::logic_layer_handler;
pub use self::detection::logic_layer_default_handler;
pub use self::shared::{Time, TimePrecision, TimeValue, LogicLayerQueryOpt, finish_aggregation};
