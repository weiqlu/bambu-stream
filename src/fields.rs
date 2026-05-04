use std::sync::Arc;

use arrow::array::{Float64Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value;

#[derive(Debug, Default, Clone)]
pub struct PrinterRow {
    pub ts_ms: i64,
    pub ts_ns: i32,
    pub bed_temper: Option<f64>,
    pub bed_target_temper: Option<f64>,
    pub nozzle_temper: Option<f64>,
    pub nozzle_target_temper: Option<f64>,
    pub chamber_temper: Option<f64>,
    pub cooling_fan_speed: Option<i64>,
    pub heatbreak_fan_speed: Option<i64>,
    pub big_fan1_speed: Option<i64>,
    pub big_fan2_speed: Option<i64>,
    pub mc_percent: Option<i64>,
    pub mc_remaining_time: Option<i64>,
    pub layer_num: Option<i64>,
    pub total_layer_num: Option<i64>,
    pub spd_mag: Option<i64>,
    pub spd_lvl: Option<i64>,
    pub print_error: Option<i64>,
    pub gcode_state: Option<String>,
    pub print_type: Option<String>,
    pub wifi_signal: Option<String>,
    pub ams_humidity: Option<i64>,
    pub ams_humidity_raw: Option<i64>,
    pub ams_temp: Option<f64>,
    pub ams_dry_time: Option<i64>,
    pub upgrade_progress: Option<i64>,
    pub upgrade_status: Option<String>,
    pub upgrade_err_code: Option<i64>,
    pub upload_progress: Option<i64>,
    pub upload_status: Option<String>,
}

impl PrinterRow {
    pub fn extract(state: &Value, ts_ms: i64, ts_ns: i32) -> Self {
        let ams_first = state
            .get("ams")
            .and_then(|a| a.get("ams"))
            .and_then(|a| a.get(0));

        Self {
            ts_ms,
            ts_ns,
            bed_temper: as_f64(state.get("bed_temper")),
            bed_target_temper: as_f64(state.get("bed_target_temper")),
            nozzle_temper: as_f64(state.get("nozzle_temper")),
            nozzle_target_temper: as_f64(state.get("nozzle_target_temper")),
            chamber_temper: as_f64(state.get("chamber_temper")),
            cooling_fan_speed: as_i64(state.get("cooling_fan_speed")),
            heatbreak_fan_speed: as_i64(state.get("heatbreak_fan_speed")),
            big_fan1_speed: as_i64(state.get("big_fan1_speed")),
            big_fan2_speed: as_i64(state.get("big_fan2_speed")),
            mc_percent: as_i64(state.get("mc_percent")),
            mc_remaining_time: as_i64(state.get("mc_remaining_time")),
            layer_num: as_i64(state.get("layer_num")),
            total_layer_num: as_i64(state.get("total_layer_num")),
            spd_mag: as_i64(state.get("spd_mag")),
            spd_lvl: as_i64(state.get("spd_lvl")),
            print_error: as_i64(state.get("print_error")),
            gcode_state: as_string(state.get("gcode_state")),
            print_type: as_string(state.get("print_type")),
            wifi_signal: as_string(state.get("wifi_signal")),
            ams_humidity: as_i64(ams_first.and_then(|a| a.get("humidity"))),
            ams_humidity_raw: as_i64(ams_first.and_then(|a| a.get("humidity_raw"))),
            ams_temp: as_f64(ams_first.and_then(|a| a.get("temp"))),
            ams_dry_time: as_i64(ams_first.and_then(|a| a.get("dry_time"))),
            upgrade_progress: as_i64(state.get("upgrade_state").and_then(|u| u.get("progress"))),
            upgrade_status: as_string(state.get("upgrade_state").and_then(|u| u.get("status"))),
            upgrade_err_code: as_i64(state.get("upgrade_state").and_then(|u| u.get("err_code"))),
            upload_progress: as_i64(state.get("upload").and_then(|u| u.get("progress"))),
            upload_status: as_string(state.get("upload").and_then(|u| u.get("status"))),
        }
    }
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ms", DataType::Int64, false),
        Field::new("ns", DataType::Int32, false),
        Field::new("bed_temper", DataType::Float64, true),
        Field::new("bed_target_temper", DataType::Float64, true),
        Field::new("nozzle_temper", DataType::Float64, true),
        Field::new("nozzle_target_temper", DataType::Float64, true),
        Field::new("chamber_temper", DataType::Float64, true),
        Field::new("cooling_fan_speed", DataType::Int64, true),
        Field::new("heatbreak_fan_speed", DataType::Int64, true),
        Field::new("big_fan1_speed", DataType::Int64, true),
        Field::new("big_fan2_speed", DataType::Int64, true),
        Field::new("mc_percent", DataType::Int64, true),
        Field::new("mc_remaining_time", DataType::Int64, true),
        Field::new("layer_num", DataType::Int64, true),
        Field::new("total_layer_num", DataType::Int64, true),
        Field::new("spd_mag", DataType::Int64, true),
        Field::new("spd_lvl", DataType::Int64, true),
        Field::new("print_error", DataType::Int64, true),
        Field::new("gcode_state", DataType::Utf8, true),
        Field::new("print_type", DataType::Utf8, true),
        Field::new("wifi_signal", DataType::Utf8, true),
        Field::new("ams_humidity", DataType::Int64, true),
        Field::new("ams_humidity_raw", DataType::Int64, true),
        Field::new("ams_temp", DataType::Float64, true),
        Field::new("ams_dry_time", DataType::Int64, true),
        Field::new("upgrade_progress", DataType::Int64, true),
        Field::new("upgrade_status", DataType::Utf8, true),
        Field::new("upgrade_err_code", DataType::Int64, true),
        Field::new("upload_progress", DataType::Int64, true),
        Field::new("upload_status", DataType::Utf8, true),
    ]))
}

pub fn to_batch(schema: SchemaRef, rows: &[PrinterRow]) -> anyhow::Result<RecordBatch> {
    let n = rows.len();

    let mut ms = Int64Builder::with_capacity(n);
    let mut ns = Int32Builder::with_capacity(n);
    let mut bed_temper = Float64Builder::with_capacity(n);
    let mut bed_target_temper = Float64Builder::with_capacity(n);
    let mut nozzle_temper = Float64Builder::with_capacity(n);
    let mut nozzle_target_temper = Float64Builder::with_capacity(n);
    let mut chamber_temper = Float64Builder::with_capacity(n);
    let mut cooling_fan_speed = Int64Builder::with_capacity(n);
    let mut heatbreak_fan_speed = Int64Builder::with_capacity(n);
    let mut big_fan1_speed = Int64Builder::with_capacity(n);
    let mut big_fan2_speed = Int64Builder::with_capacity(n);
    let mut mc_percent = Int64Builder::with_capacity(n);
    let mut mc_remaining_time = Int64Builder::with_capacity(n);
    let mut layer_num = Int64Builder::with_capacity(n);
    let mut total_layer_num = Int64Builder::with_capacity(n);
    let mut spd_mag = Int64Builder::with_capacity(n);
    let mut spd_lvl = Int64Builder::with_capacity(n);
    let mut print_error = Int64Builder::with_capacity(n);
    let mut gcode_state = StringBuilder::new();
    let mut print_type = StringBuilder::new();
    let mut wifi_signal = StringBuilder::new();
    let mut ams_humidity = Int64Builder::with_capacity(n);
    let mut ams_humidity_raw = Int64Builder::with_capacity(n);
    let mut ams_temp = Float64Builder::with_capacity(n);
    let mut ams_dry_time = Int64Builder::with_capacity(n);
    let mut upgrade_progress = Int64Builder::with_capacity(n);
    let mut upgrade_status = StringBuilder::new();
    let mut upgrade_err_code = Int64Builder::with_capacity(n);
    let mut upload_progress = Int64Builder::with_capacity(n);
    let mut upload_status = StringBuilder::new();

    for r in rows {
        ms.append_value(r.ts_ms);
        ns.append_value(r.ts_ns);
        bed_temper.append_option(r.bed_temper);
        bed_target_temper.append_option(r.bed_target_temper);
        nozzle_temper.append_option(r.nozzle_temper);
        nozzle_target_temper.append_option(r.nozzle_target_temper);
        chamber_temper.append_option(r.chamber_temper);
        cooling_fan_speed.append_option(r.cooling_fan_speed);
        heatbreak_fan_speed.append_option(r.heatbreak_fan_speed);
        big_fan1_speed.append_option(r.big_fan1_speed);
        big_fan2_speed.append_option(r.big_fan2_speed);
        mc_percent.append_option(r.mc_percent);
        mc_remaining_time.append_option(r.mc_remaining_time);
        layer_num.append_option(r.layer_num);
        total_layer_num.append_option(r.total_layer_num);
        spd_mag.append_option(r.spd_mag);
        spd_lvl.append_option(r.spd_lvl);
        print_error.append_option(r.print_error);
        gcode_state.append_option(r.gcode_state.as_deref());
        print_type.append_option(r.print_type.as_deref());
        wifi_signal.append_option(r.wifi_signal.as_deref());
        ams_humidity.append_option(r.ams_humidity);
        ams_humidity_raw.append_option(r.ams_humidity_raw);
        ams_temp.append_option(r.ams_temp);
        ams_dry_time.append_option(r.ams_dry_time);
        upgrade_progress.append_option(r.upgrade_progress);
        upgrade_status.append_option(r.upgrade_status.as_deref());
        upgrade_err_code.append_option(r.upgrade_err_code);
        upload_progress.append_option(r.upload_progress);
        upload_status.append_option(r.upload_status.as_deref());
    }

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ms.finish()),
            Arc::new(ns.finish()),
            Arc::new(bed_temper.finish()),
            Arc::new(bed_target_temper.finish()),
            Arc::new(nozzle_temper.finish()),
            Arc::new(nozzle_target_temper.finish()),
            Arc::new(chamber_temper.finish()),
            Arc::new(cooling_fan_speed.finish()),
            Arc::new(heatbreak_fan_speed.finish()),
            Arc::new(big_fan1_speed.finish()),
            Arc::new(big_fan2_speed.finish()),
            Arc::new(mc_percent.finish()),
            Arc::new(mc_remaining_time.finish()),
            Arc::new(layer_num.finish()),
            Arc::new(total_layer_num.finish()),
            Arc::new(spd_mag.finish()),
            Arc::new(spd_lvl.finish()),
            Arc::new(print_error.finish()),
            Arc::new(gcode_state.finish()),
            Arc::new(print_type.finish()),
            Arc::new(wifi_signal.finish()),
            Arc::new(ams_humidity.finish()),
            Arc::new(ams_humidity_raw.finish()),
            Arc::new(ams_temp.finish()),
            Arc::new(ams_dry_time.finish()),
            Arc::new(upgrade_progress.finish()),
            Arc::new(upgrade_status.finish()),
            Arc::new(upgrade_err_code.finish()),
            Arc::new(upload_progress.finish()),
            Arc::new(upload_status.finish()),
        ],
    )?)
}

fn as_f64(v: Option<&Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str()?.parse().ok())
}

fn as_i64(v: Option<&Value>) -> Option<i64> {
    let v = v?;
    v.as_i64()
        .or_else(|| v.as_str()?.parse().ok())
        .or_else(|| v.as_f64().map(|f| f as i64))
}

fn as_string(v: Option<&Value>) -> Option<String> {
    v?.as_str().map(String::from)
}
