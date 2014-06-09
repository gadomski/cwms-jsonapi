CREATE OR REPLACE PACKAGE JSONAPI AS 

  TYPE string_string_hash IS TABLE OF VARCHAR2(200) INDEX BY VARCHAR2(64);

  PROCEDURE locations(db_office_id IN VARCHAR2,
                      unit_system IN VARCHAR2 DEFAULT 'SI');

  PROCEDURE locations(location_id IN VARCHAR2,
                unit_system IN VARCHAR2 DEFAULT 'SI');

  PROCEDURE timeseries(location_id IN VARCHAR2);

  PROCEDURE timeseries(ts_code IN NUMBER);
    
  PROCEDURE timeseriesdata(ts_code IN NUMBER);
  
END JSONAPI;
/


CREATE OR REPLACE PACKAGE BODY JSONAPI AS

  FUNCTION quote(value_ IN VARCHAR2)
  RETURN VARCHAR2 AS
  BEGIN
    return '"' || value_ || '"';
  END quote;

  FUNCTION format_date(value_ IN DATE)
  RETURN VARCHAR2 AS
  BEGIN
    return quote(TO_CHAR(value_, 'IYYY-MM-DD') || 'T' || TO_CHAR(value_, 'HH24:MI:SS'));
  END format_date;
  
  FUNCTION or_null(value_ IN NUMBER)
  RETURN VARCHAR2 AS
  BEGIN
    IF value_ IS NULL THEN
      return 'null';
    ELSE
      return value_;
    END IF;
  END or_null;
  
  PROCEDURE htp_json_header AS
  BEGIN
    owa_util.mime_header('application/json');
  END htp_json_header;
  
  PROCEDURE htp_json_array_open AS
  BEGIN
    htp_json_header();
    htp.prn('[');
  END htp_json_array_open;
  
  PROCEDURE htp_json_array_close AS
  BEGIN
    htp.prn(']');
  END htp_json_array_close;
  
  PROCEDURE htp_json_object_open AS
  BEGIN
    htp_json_header();
    htp.prn('{');
  END htp_json_object_open;
  
  PROCEDURE htp_json_object_close AS
  BEGIN
    htp.prn('}');
  END htp_json_object_close;
  
  PROCEDURE htp_json_object(data_ string_string_hash,
                            include_brackets IN BOOLEAN DEFAULT TRUE) AS
    first_loop BOOLEAN;
    key_ VARCHAR(64);
  BEGIN
    first_loop := TRUE;
    IF include_brackets THEN
      htp.prn('{');
    END IF;
    key_ := data_.FIRST;
    WHILE key_ IS NOT NULL LOOP
      htp.prn('"' || key_ || '":' || data_(key_));
      key_ := data_.NEXT(key_);
      IF key_ IS NOT NULL THEN
        htp.prn(',');
      END IF;
    END LOOP;
    IF include_brackets THEN
      htp.prn('}');
    END IF;
  END htp_json_object;

  PROCEDURE locations(db_office_id IN VARCHAR2,
                      unit_system IN VARCHAR2 DEFAULT 'SI') AS
    data_ string_string_hash;
    first_loop BOOLEAN := TRUE;
  BEGIN
    htp_json_array_open();
    FOR location_ IN
    (
      SELECT
        l.db_office_id,
        l.location_id,
        l.location_type,
        l.unit_system,
        l.elevation,
        l.unit_id,
        l.longitude,
        l.latitude,
        l.time_zone_name,
        l.state_initial,
        l.long_name,
        l.active_flag
      FROM cwms_v_loc l
      WHERE l.db_office_id = locations.db_office_id
        AND l.unit_system = locations.unit_system
    )
    LOOP
      IF NOT first_loop THEN
        htp.prn(',');
      ELSE
        first_loop := false;
      END IF;
      data_('db_office_id') := quote(location_.db_office_id);
      data_('location_id') := quote(location_.location_id);
      data_('location_type') := quote(location_.location_type);
      data_('unit_system') := quote(location_.unit_system);
      data_('elevation') := or_null(location_.elevation);
      data_('unit_id') := quote(location_.unit_id);
      data_('longitude') := location_.longitude;
      data_('latitude') := location_.latitude;
      data_('time_zone_name') := quote(location_.time_zone_name);
      data_('state_initial') := quote(location_.state_initial);
      data_('long_name') := quote(location_.long_name);
      data_('active_flag') := quote(location_.active_flag);
      htp_json_object(data_);
    END LOOP;
    htp_json_array_close();
  END locations;
  
  PROCEDURE locations(location_id IN VARCHAR2,
                unit_system IN VARCHAR2 DEFAULT 'SI') AS
    data_ string_string_hash;
    row_ cwms_v_loc%rowtype;
  BEGIN
    htp_json_object_open();
    SELECT *
      INTO row_
      FROM cwms_v_loc l
      WHERE l.location_id = locations.location_id
        AND l.unit_system = locations.unit_system;
    
    data_('db_office_id') := quote(row_.db_office_id);
    data_('location_id') := quote(row_.location_id);
    data_('location_type') := quote(row_.location_type);
    data_('unit_system') := quote(row_.unit_system);
    data_('elevation') := row_.elevation;
    data_('unit_id') := quote(row_.unit_id);
    data_('longtitude') := row_.longitude;
    data_('latitude') := row_.latitude;
    data_('time_zone_name') := quote(row_.time_zone_name);
    data_('state_initial') := quote(row_.state_initial);
    data_('long_name') := quote(row_.long_name);
    data_('active_flag') := quote(row_.active_flag);
    
    htp_json_object(data_, FALSE);
    htp_json_object_close();
  END locations;
  
  PROCEDURE timeseries(location_id IN VARCHAR2) AS
    data_ string_string_hash;
    first_loop BOOLEAN := TRUE;
  BEGIN
    htp_json_array_open();
    FOR ts IN
    (
      SELECT
        t.db_office_id,
        t.location_id,
        t.cwms_ts_id,
        t.unit_id,
        t.parameter_id,
        t.ts_code
      FROM cwms_v_ts_id t
      WHERE t.location_id = timeseries.location_id
    )
    LOOP
      IF NOT first_loop THEN
        htp.prn(',');
      ELSE
        first_loop := false;
      END IF;
      data_('db_office_id') := quote(ts.db_office_id);
      data_('location_id') := quote(ts.location_id);
      data_('cwms_ts_id') := quote(ts.cwms_ts_id);
      data_('unit_id') := quote(ts.unit_id);
      data_('parameter_id') := quote(ts.parameter_id);
      data_('ts_code') := ts.ts_code;
      htp_json_object(data_);
    END LOOP;
    htp_json_array_close();
  END timeseries;
  
  PROCEDURE timeseries(ts_code IN NUMBER) AS
    data_ string_string_hash;
    row_ cwms_v_ts_id%rowtype;
  BEGIN
    htp_json_object_open();
    SELECT *
      INTO row_
      FROM cwms_v_ts_id t
      WHERE t.ts_code = timeseries.ts_code;

    data_('db_office_id') := quote(row_.db_office_id);
    data_('location_id') := quote(row_.location_id);
    data_('cwms_ts_id') := quote(row_.cwms_ts_id);
    data_('unit_id') := quote(row_.unit_id);
    data_('parameter_id') := quote(row_.parameter_id);
    data_('ts_code') := row_.ts_code;
    htp_json_object(data_, FALSE);
    htp_json_object_close();
  END timeseries;
  
  PROCEDURE timeseriesdata(ts_code IN NUMBER) AS
    data_ string_string_hash;
    first_loop BOOLEAN := TRUE;
  BEGIN
    htp_json_array_open();
    FOR ts IN
    (
      SELECT
        t.ts_code,
        t.date_time,
        t.value,
        t.quality_code
      FROM cwms_v_tsv t
      WHERE t.ts_code = timeseriesdata.ts_code
    )
    LOOP
      IF NOT first_loop THEN
        htp.prn(',');
      ELSE
        first_loop := false;
      END IF;
      data_('ts_code') := ts.ts_code;
      data_('date_time') := format_date(ts.date_time);
      data_('value') := or_null(ts.value);
      data_('quality_code') := ts.quality_code;
      htp_json_object(data_);
    END LOOP;
    htp_json_array_close();
  END timeseriesdata;

END JSONAPI;
/
