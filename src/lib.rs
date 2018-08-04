//extern crate serde;
//extern crate serde_qs;
//extern crate warp;
//
//// Query Filters -- nested
//
//use serde::de::DeserializeOwned;
////use serde_qs;
//
//use warp::filter::{Filter, filter_fn_one, One};
//use warp::reject::{self, Rejection};
//
///// Creates a `Filter` that decodes query parameters to the type `T`.
/////
///// If cannot decode into a `T`, the request is rejected with a `400 Bad Request`.
//pub fn query_nested<T: DeserializeOwned + Send>() -> impl Filter<Extract=One<T>, Error=Rejection> + Copy {
//    filter_fn_one(|route| {
//        route
//            .query()
//            .and_then(|q| {
//                serde_qs::from_str(q)
//                    .ok()
//            })
//            .map(Ok)
//            .unwrap_or_else(|| Err(reject::bad_request()))
//    })
//}
//#[cfg(test)]
//mod tests {
//    #[test]
//    fn it_works() {
//        assert_eq!(2 + 2, 4);
//    }
//}
