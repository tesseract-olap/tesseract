use tesseract_core::Aggregator;

pub fn agg_sql_string(col: &str, aggregator: &Aggregator) -> String {
    match aggregator {
        Aggregator::Sum => format!("sum({})", col),
        Aggregator::Average => format!("avg({})", col),
        Aggregator::Median => format!("median({})", col),
        // TODO impl this
        Aggregator::WeightedAverage => format!("avg({})", col),
        Aggregator::Moe => format!("sqrt(sum(power({} / 1.645, 2))) * 1.645", col),
        // TODO uses find and replace; the placeholder is {}
        Aggregator::Custom(s) => format!("{}{}", s, col),
    }
}
