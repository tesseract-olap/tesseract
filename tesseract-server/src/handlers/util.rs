use actix_web::http::header::ContentType;
use mime;
use tesseract_core::format::FormatType;

pub(crate) fn format_to_content_type(format_type: &FormatType) -> ContentType {
    match format_type {
        FormatType::Csv => ContentType(mime::TEXT_CSV_UTF_8),
        FormatType::JsonRecords => ContentType(mime::APPLICATION_JSON),
        FormatType::JsonArrays => ContentType(mime::APPLICATION_JSON),
    }
}
