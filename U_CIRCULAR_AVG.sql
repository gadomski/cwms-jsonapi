--------------------------------------------------------
--  File created - Thursday-September-18-2014   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Type U_CIRCULAR_AVG
--------------------------------------------------------

  CREATE OR REPLACE TYPE "CWMS_WEB"."U_CIRCULAR_AVG" AS OBJECT
(
    running_sum_cos_n NUMBER, -- running sum of the cosine of the numbers passed
    running_sum_sin_n NUMBER, -- running sum of the sine of the numbers passed
    running_count NUMBER, -- count of the numbers passed
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT U_CIRCULAR_AVG) 
        RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateIterate(self IN OUT U_CIRCULAR_AVG, 
        value IN NUMBER) RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateTerminate(self IN U_CIRCULAR_AVG, 
        returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateMerge(self IN OUT U_CIRCULAR_AVG, 
        ctx2 IN U_CIRCULAR_AVG) RETURN NUMBER
);
/
CREATE OR REPLACE TYPE BODY "CWMS_WEB"."U_CIRCULAR_AVG" IS

  STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT U_CIRCULAR_AVG) 
    RETURN NUMBER IS
  BEGIN
    SCTX := U_CIRCULAR_AVG(0, 0, 0);
    RETURN ODCIConst.Success;
  END;
  
  MEMBER FUNCTION ODCIAggregateIterate(self IN OUT U_CIRCULAR_AVG, 
    value IN NUMBER) RETURN NUMBER IS
  BEGIN
    SELF.running_sum_cos_n := SELF.running_sum_cos_n + 
        COS(value*3.14159265359/180);
    SELF.running_sum_sin_n := SELF.running_sum_sin_n + 
        SIN(value*3.14159265359/180);
    SELF.running_count := SELF.running_count + 1;
    RETURN ODCIConst.Success;
  END;
  
  MEMBER FUNCTION ODCIAggregateTerminate(self IN U_CIRCULAR_AVG, 
        returnValue OUT NUMBER, flags IN NUMBER) RETURN NUMBER IS
    avg_c number;
    avg_s number;
    n number;
  BEGIN
    avg_c := SELF.running_sum_cos_n / SELF.running_count;
    avg_s := SELF.running_sum_sin_n / SELF.running_count;
    n := ATAN2(avg_s, avg_c) * 180 / 3.14159265359;

    IF n >= 0 THEN
        returnValue := n;
    ELSE 
        returnValue := n + 360;
    END IF;

    RETURN ODCIConst.Success;
  END;
  
  MEMBER FUNCTION ODCIAggregateMerge(self IN OUT U_CIRCULAR_AVG, 
    ctx2 IN U_CIRCULAR_AVG) RETURN NUMBER IS
  BEGIN
    SELF.running_sum_cos_n := SELF.running_sum_cos_n + 
        ctx2.running_sum_cos_n;
    SELF.running_sum_sin_n := SELF.running_sum_sin_n + 
        ctx2.running_sum_sin_n;
    SELF.running_count := SELF.running_count + ctx2.running_count;
    RETURN ODCIConst.Success;
  END;
END;

/
