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

    let mut wtr = Writer::from_path("test_data_2.csv")?;

    for year in 2009..2018 {
        for tract in 0..73000 {
            for age in 0..10 {
                for race in 0..8 {
                //for sex in 0..2 {
                    wtr.serialize(
                        Row {
                            year,
                            tract,
                            age,
                            race,
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
    race: usize,
    population: usize,
}
