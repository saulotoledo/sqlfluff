-- EXECUTE IMMEDIATE Patterns Test File
-- Based on test/fixtures/dialects/oracle/plsql_block.sql

-- Pattern 1: Basic EXECUTE IMMEDIATE with concatenation
DECLARE
    constraint_name VARCHAR2(255);
    result_var NUMBER;
    table_name VARCHAR2(100) := 'MY_TABLE';
BEGIN
    SELECT constraint_name
    INTO constraint_name
    FROM user_constraints
    WHERE table_name = 'MY_TABLE'
        AND constraint_type = 'C'
        AND search_condition_vc LIKE 'MY_CONDITION%';

    EXECUTE IMMEDIATE 'ALTER TABLE ' || table_name ||
        ' DROP CONSTRAINT ' || constraint_name;
    EXECUTE IMMEDIATE 'ALTER TABLE MY_TABLE2 DROP CONSTRAINT ' ||
        constraint_name;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || table_name
        INTO result_var;
    EXECUTE IMMEDIATE 'INSERT INTO MY_TABLE3 VALUES (:1, :2)'
        USING constraint_name, result_var;
    EXECUTE IMMEDIATE 'DROP TABLE MY_TABLE';
END;
/

-- Pattern 2: EXECUTE IMMEDIATE with variable expression
DECLARE
    a NUMBER := 4;
    b NUMBER := 7;
    plsql_block VARCHAR2(100);
BEGIN
    plsql_block := 'BEGIN calc_stats(:x, :x, :y, :x); END;';
    EXECUTE IMMEDIATE plsql_block USING a, b;
END;
/

-- Pattern 3: EXECUTE IMMEDIATE with multi-line USING clause
DECLARE
    a_null CHAR(1);
BEGIN
    EXECUTE IMMEDIATE 'UPDATE employees_temp SET commission_pct = :x'
        USING a_null;
END;
/

-- Pattern 4: EXECUTE IMMEDIATE with IN OUT parameter modes
DECLARE
    plsql_block VARCHAR2(500);
    new_deptid NUMBER(4);
    new_dname VARCHAR2(30) := 'Advertising';
    new_mgrid NUMBER(6) := 200;
    new_locid NUMBER(4) := 1700;
BEGIN
    plsql_block := 'BEGIN create_dept(:a, :b, :c, :d); END;';
    EXECUTE IMMEDIATE plsql_block
        USING IN OUT new_deptid, new_dname, new_mgrid, new_locid;
END;
/

-- Pattern 5: EXECUTE IMMEDIATE with simple variable
DECLARE
    dyn_stmt VARCHAR2(200);
    b BOOLEAN := TRUE;
BEGIN
    dyn_stmt := 'BEGIN p(:x); END;';
    EXECUTE IMMEDIATE dyn_stmt USING b;
END;
/

-- Pattern 6: EXECUTE IMMEDIATE with OUT parameter mode
DECLARE
    r pkg.rec;
    dyn_str VARCHAR2(3000);
BEGIN
    dyn_str := 'BEGIN pkg.p(:x, 6, 8); END;';
    EXECUTE IMMEDIATE dyn_str USING OUT r;
    DBMS_OUTPUT.PUT_LINE('r.n1 = ' || r.n1);
    DBMS_OUTPUT.PUT_LINE('r.n2 = ' || r.n2);
END;
/

-- Pattern 7: EXECUTE IMMEDIATE with INTO clause
DECLARE
    v_count NUMBER;
    v_table VARCHAR2(100) := 'employees';
BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table
        INTO v_count;
    DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
END;
/

-- Pattern 8: EXECUTE IMMEDIATE with RETURNING INTO
DECLARE
    v_emp_id NUMBER := 100;
    v_new_salary NUMBER := 7500;
    v_old_salary NUMBER;
BEGIN
    EXECUTE IMMEDIATE 'UPDATE employees SET salary = :new_sal ' ||
        'WHERE employee_id = :emp_id ' ||
        'RETURNING salary INTO :old_sal'
        USING v_new_salary, v_emp_id, OUT v_old_salary;
    
    DBMS_OUTPUT.PUT_LINE('Old salary: ' || v_old_salary);
    DBMS_OUTPUT.PUT_LINE('New salary: ' || v_new_salary);
END;
/

-- Pattern 9: Complex multi-line EXECUTE IMMEDIATE with concatenation
DECLARE
    v_sql VARCHAR2(1000);
    v_table VARCHAR2(100) := 'employees';
    v_dept_id NUMBER := 10;
    v_min_salary NUMBER := 5000;
    v_result NUMBER;
BEGIN
    v_sql := 'SELECT COUNT(*) FROM ' || v_table ||
        ' WHERE department_id = :dept_id' ||
        ' AND salary >= :min_salary';
    
    EXECUTE IMMEDIATE v_sql
        INTO v_result
        USING v_dept_id, v_min_salary;
    
    DBMS_OUTPUT.PUT_LINE('Result: ' || v_result);
END;
/ 