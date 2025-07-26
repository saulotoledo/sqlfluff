CreaTE SEQUENCE seq_mY_TabLe START WITH 1;

create TaBlE mY_Table
(
  id NUMBER(18) DEFAULT seq_mY_TabLe.NEXTVAL,
  name VARCHAR2(100),
  description VARCHAR2(255),
  created_at timestamp DEFAULT current_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CONStrAINT my_table_pk PRIMARY KEY (id)
);
