use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyString};

use regex::Regex;
use std::{
    fmt::Write,
    fs::File,
    io::{self, BufRead, BufReader, Cursor, Read},
    ops::{Add, Mul},
    slice,
};

use byteorder::{LittleEndian, ReadBytesExt};
use polars::series::Series;
use polars::{
    export::rayon::prelude::{IndexedParallelIterator, IntoParallelIterator},
    prelude::*,
};
use polars::{frame::DataFrame, prelude::DataType};

use polars::prelude::*;
/// Reads in a .dat and a .hdr file and converts them into a csv file with the proper columns.
/// Also it is taken care of the physical scale (the interlaced i16 values in the dat file are scaled with slope and intercept)
#[pyfunction]
pub fn gx1_to_parquet(dat_file: &str, hdr_file: &str, out_file: &str) -> PyResult<String> {
    gx1_to_parquet_internal(dat_file, hdr_file, out_file)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

pub fn gx1_to_parquet_internal(
    dat_file: &str,
    hdr_file: &str,
    out_file: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let header_file = File::open(hdr_file)?;
    let header_reader = BufReader::new(header_file);

    let mut header = String::new();
    for line in header_reader.lines() {
        if let Ok(line) = line {
            writeln!(header, "{}", line).unwrap();
        }
        // just ignore error lines...
    }

    // find out how many columns:
    let columns_re = Regex::new(r"SERIES ([^\n]+)\n").unwrap();
    let columns = columns_re
        .captures(&header)
        .unwrap()
        .get(1)
        .unwrap()
        .as_str()
        .trim()
        .split(", ")
        .collect::<Vec<_>>();

    // get slope values
    let slope_re = Regex::new(r"SLOPE ([^\n]+)\n").unwrap();
    let slopes = slope_re
        .captures(&header)
        .unwrap()
        .get(1)
        .unwrap()
        .as_str()
        .trim()
        .split(", ")
        .map(|s| s.parse::<f32>().unwrap())
        .collect::<Vec<_>>();

    // get y_offset_values
    let y_offset_re = Regex::new(r"Y_OFFSET ([^\n]+)\n").unwrap();
    let y_offsets = y_offset_re
        .captures(&header)
        .unwrap()
        .get(1)
        .unwrap()
        .as_str()
        .trim()
        .split(", ")
        .map(|s| s.parse::<f32>().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(columns.len(), slopes.len());
    assert_eq!(slopes.len(), y_offsets.len());

    let data_i16 = read_binary_file_to_vec(dat_file)?;
    let mut series_vecs: Vec<(String, Vec<f32>)> =
        columns.iter().map(|s| (s.to_string(), vec![])).collect();
    let col_len = columns.len();
    for (i, num) in data_i16.into_iter().enumerate() {
        series_vecs[i % col_len].1.push(num as f32);
    }

    let series: Vec<Series> = series_vecs
        .into_iter()
        .enumerate()
        .map(|(i, (s, nums))| Series::from_vec(&s, nums).mul(slopes[i]).add(y_offsets[i]))
        .collect();

    let mut frame = DataFrame::new(series).unwrap(); //.head(Some(10));

    let mut out_file: File = File::create(out_file).expect("could not create file");
    ParquetWriter::new(&mut out_file).finish(&mut frame).ok();
    // let debug_output = format!("{:?}", frame);
    // // std::fs::write("out.txt", debug_output).expect("Failed to write to file");

    // Ok(debug_output.into())
    Ok("Success".into())
}

fn read_binary_file_to_vec(path: &str) -> io::Result<Vec<i16>> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;

    let mut rdr = Cursor::new(buf);
    let mut vec = Vec::new();

    while let Ok(i) = rdr.read_i16::<LittleEndian>() {
        vec.push(i);
    }

    Ok(vec)
}

/// A Python module implemented in Rust.
#[pymodule]
fn gx1convert(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(gx1_to_parquet, m)?)?;
    Ok(())
}

#[cfg(test)]
#[test]
pub fn foo() {
    let res = gx1_to_parquet_internal(
        "../code/data/D0400001.dat",
        "../code/data/D0400001.hdr",
        "lul.parquet",
    );
    dbg!(res);
}
