CREATE OR REPLACE PROCEDURE db2perf_quiet_drop( IN statement VARCHAR(1000) )
LANGUAGE SQL
BEGIN
DECLARE SQLSTATE CHAR(5);
DECLARE NotThere    CONDITION FOR SQLSTATE '42704';
DECLARE NotThereSig CONDITION FOR SQLSTATE '42883';

DECLARE EXIT HANDLER FOR NotThere, NotThereSig
SET SQLSTATE = '     ';

SET statement = 'DROP ' || statement;
EXECUTE IMMEDIATE statement;
END

BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE customer');
END


BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE district');
END


BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE history');
END

BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE oorder');
END

BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE order_line');
END

BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE stock');
END

BEGIN ATOMIC
CALL db2perf_quiet_drop('TABLE warehouse');
END
