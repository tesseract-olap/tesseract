use failure::{Error, format_err, bail};
use std::str::FromStr;

use crate::names::{
    Cut,
    Drilldown,
    Measure,
    Property,
    LevelName,
};

#[derive(Debug, Clone)]
pub struct Query {
    pub cuts: Vec<Cut>,
    pub drilldowns: Vec<Drilldown>,
    pub measures: Vec<Measure>,
    pub properties: Vec<Property>,
    pub parents: bool,
    pub top: Option<TopQuery>,
    pub sort: Option<SortQuery>,
    pub limit: Option<LimitQuery>,
    pub rca: Option<RcaQuery>,
    pub growth: Option<GrowthQuery>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            drilldowns: vec![],
            cuts: vec![],
            measures: vec![],
            properties: vec![],
            parents: false,
            top: None,
            sort: None,
            limit: None,
            rca: None,
            growth: None,
        }
    }
}

/// Clickhouse:
/// select * from table_name order by sort_measures sort_direction
/// limit n by by_dimension
#[derive(Debug, Clone)]
pub struct TopQuery {
    pub n: u64,
    pub by_dimension: LevelName,
    pub sort_mea_or_calc: Vec<MeaOrCalc>,
    pub sort_direction: SortDirection,
}

// Currently only allows one sort_measure
impl FromStr for TopQuery {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.split(",").collect::<Vec<_>>()[..] {
            [n, by_dimension, sort_measure, sort_direction] => {

                let n = n.parse::<u64>()?;
                let by_dimension = by_dimension.parse::<LevelName>()?;
                let sort_mea_or_calc = vec![sort_measure.parse::<MeaOrCalc>()?];
                let sort_direction = sort_direction.parse::<SortDirection>()?;

                Ok(TopQuery {
                    n,
                    by_dimension,
                    sort_mea_or_calc,
                    sort_direction,
                })
            },
            _ => bail!("Could not parse a top query"),
        }
    }
}

// Just for TopQuery
/// Currently rca and growth will be reserved keywords. This may be changed in the future,
/// to allow measures that are named rca and growth
#[derive(Debug, Clone)]
pub enum MeaOrCalc {
    Mea(Measure),
    Calc(Calculation),
}

impl FromStr for MeaOrCalc {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Calculation>()
            .map(|c| MeaOrCalc::Calc(c))
            .or_else(|_| {
                s.parse::<Measure>()
                    .map(|m| MeaOrCalc::Mea(m))
            })
            .map_err(|_| format_err!("Could not parse '{}' to measure name or built-in calculation name", s))
    }
}

#[derive(Debug, Clone)]
pub enum Calculation {
    Rca,
    Growth,
}

impl Calculation {
    pub(crate) fn sql_string(&self) -> String {
        match self {
            Calculation::Rca => "rca".to_owned(),
            Calculation::Growth => "growth".to_owned(),
        }
    }
}

impl FromStr for Calculation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.to_lowercase()[..] {
            "rca" => Ok(Calculation::Rca),
            "growth" => Ok(Calculation::Growth),
            _ => Err(format_err!("'{}' is not a supported calculation", s)),
        }
    }
}


#[derive(Debug, Clone)]
pub struct LimitQuery {
    pub n: u64,
}

impl FromStr for LimitQuery {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(LimitQuery { n: s.parse::<u64>()? })
    }
}

#[derive(Debug, Clone)]
pub struct SortQuery {
    pub direction: SortDirection,
    pub measure: Measure,
}

impl FromStr for SortQuery {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.split(".").collect::<Vec<_>>()[..] {
            [measure, direction] => {
                let measure = measure.parse::<Measure>()?;
                let direction = direction.parse::<SortDirection>()?;
                Ok(SortQuery {
                    direction,
                    measure,
                })
            },
            _ => bail!("Could not parse a sort query"),
        }

    }
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    pub fn sql_string(&self) -> String {
        match *self {
            SortDirection::Asc => "asc".to_owned(),
            SortDirection::Desc => "desc".to_owned(),
        }
    }
}

impl FromStr for SortDirection {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "asc" => SortDirection::Asc,
            "desc" => SortDirection::Desc,
            _ => bail!("Could not parse sort direction"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RcaQuery {
    pub drill_1: Drilldown,
    pub drill_2: Drilldown,
    pub mea: Measure,
}

impl FromStr for RcaQuery {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.split(",").collect::<Vec<_>>()[..] {
            [drill_1, drill_2, measure] => {
                let drill_1 = drill_1.parse::<Drilldown>()?;
                let drill_2 = drill_2.parse::<Drilldown>()?;
                let mea = measure.parse::<Measure>()?;

                Ok(RcaQuery {
                    drill_1,
                    drill_2,
                    mea,
                })
            },
            _ => bail!("Could not parse an rca query, wrong number of args"),
        }

    }
}

#[derive(Debug, Clone)]
pub struct GrowthQuery {
    pub time_drill: Drilldown,
    pub mea: Measure,
}

impl FromStr for GrowthQuery {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.split(",").collect::<Vec<_>>()[..] {
            [time_drill, measure] => {
                let time_drill = time_drill.parse::<Drilldown>()?;
                let mea = measure.parse::<Measure>()?;

                Ok(GrowthQuery {
                    time_drill,
                    mea,
                })
            },
            _ => bail!("Could not parse a growth query, wrong number of args"),
        }

    }
}
