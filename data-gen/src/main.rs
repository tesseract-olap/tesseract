extern crate csv;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use csv::Writer;
use rand::{thread_rng, Rng};

// hard-coding in some large-ish data sets for testing
fn main() -> Result<(), Box<std::error::Error>> {
    let mut rng = thread_rng();

    let mut wtr = Writer::from_path("test_data.csv")?;

    for year in 2009..2018 {
        for tract in 0..73000 {
            for age in 0..10 {
                for sex in 0..2 {
                    wtr.serialize(
                        Row {
                            year,
                            tract,
                            age,
                            sex,
                            population: rng.gen_range(0, 10_000_000),
                        }
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct Row {
    year: usize,
    tract: usize,
    age: usize,
    sex: usize,
    population: usize,
}
