DROP SCHEMA IF EXISTS stage cascade;
DROP SCHEMA IF EXISTS ds cascade;
DROP SCHEMA IF EXISTS logs cascade;
DROP SCHEMA IF EXISTS dm cascade;

CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS logs;
CREATE SCHEMA IF NOT EXISTS dm;



CREATE TABLE ds.ft_balance_f(
	on_date date NOT NULL,
	account_rk bigint NOT NULL,
	currency_rk int4, 
	balance_out float,
CONSTRAINT p_key_dt_balance PRIMARY KEY (on_date,account_rk));

CREATE TABLE ds.ft_posting_f(
	oper_date date NOT NULL,
	credit_account_rk bigint NOT NULL,
	debet_account_rk bigint NOT NULL,
	credit_amount float,
	debet_amount float);

CREATE TABLE ds.md_account_d(
	data_actual_date date NOT NULL,
	data_actual_end_date date NOT NULL,
	account_rk bigint NOT NULL,
	account_number varchar(20) NOT NULL,
	char_type varchar(1) NOT NULL,
	currency_rk bigint NOT NULL,
	currency_code varchar(3) NOT NULL,
CONSTRAINT p_key_md_account_d PRIMARY KEY (data_actual_date,account_rk));

CREATE TABLE ds.md_currency_d(
	currency_rk bigint NOT NULL,
	data_actual_date date NOT NULL,
	data_actual_end_date date ,
	currency_code varchar(3),
	code_iso_char varchar(3),
CONSTRAINT p_key_md_currency_d PRIMARY KEY (currency_rk,data_actual_date));

CREATE TABLE ds.md_exchange_rate_d(
	data_actual_date date NOT NULL,
	data_actual_end_date date,
	currency_rk bigint NOT NULL,
	reduced_cource float,
	code_iso_num varchar(3),
CONSTRAINT p_key_md_exchange_rate_d PRIMARY KEY (data_actual_date,currency_rk));

CREATE TABLE ds.md_ledger_account_s(
	chapter varchar(1),
	chapter_name varchar(16),
	section_number integer,
	section_name varchar(22),
	subsection_name varchar(21),
	ledger1_account integer,
	ledger1_account_name varchar(47),
	ledger_account integer NOT NULL,
	ledger_account_name varchar(153),
	characteristic char(1),
	is_resident integer,
	is_reserve integer,
	is_reserved integer,
	is_loan integer,
	id_reserved_assets integer,
	id_overdue integer,
	is_interest integer,
	pair_account varchar(5),
	start_date date NOT NULL,
	end_date date,
	is_rub_only integer,
	mit_term varchar(1),
	min_term_measure varchar(1),
	max_term varchar(1),
	max_term_measure varchar(1),
	ledger_acc_ful_name_tarnsit varchar(1),
	is_revaluation varchar(1),
	is_correct varchar(1),
CONSTRAINT p_key_ledger_account_s PRIMARY KEY (ledger_account,start_date));

CREATE TABLE dm.account_turnover_f(
	on_date date,
	account_rk bigint,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8));

CREATE TABLE dm.dm_f101_round_f(
	from_date date,
	to_date date,
	chapter varchar(1),
	ledger_account varchar(5),
	charasteristic varchar(1),
	r_balance_in_rub numeric(23,8),
	balance_in_val numeric(23,8),
	r_balance_in_val numeric(23,8),
	balance_in_total numeric(23,8),
	r_balance_in_total numeric(23,8),
	turn_deb_rub numeric(23,8),
	r_turn_deb_rub numeric(23,8),
	turn_deb_val numeric(23,8),
	r_turn_deb_val numeric(23,8),
	turn_deb_total numeric(23,8),
	r_turn_deb_total numeric(23,8),
	turn_cre_rub numeric(23,8),
	r_turn_cre_rub numeric(23,8),
	turn_cre_val numeric(23,8),
	r_turn_cre_val numeric(23,8),
	turn_cre_total numeric(23,8),
	r_turn_cre_total numeric(23,8),
	balance_out_rub numeric(23,8),
	r_balance_out_rub numeric(23,8),
	balance_out_val numeric(23,8),
	r_balance_out_val numeric(23,8),
	balance_out_total numeric(23,8),
	r_balance_out_total numeric(23,8));

CREATE TABLE logs.log_stg(
	start_etl timestamptz NOT NULL,
	table_name varchar(100),
	schema_name varchar(100),
	status varchar(25),
	message varchar(255),
	end_etl timestamptz NOT null);


CREATE OR REPLACE PROCEDURE ds.etl_stage() --Процедура по загрузки из CSV в stage ==> ds
LANGUAGE plpgsql 
AS $$
DECLARE 
	start_etl_t timestamptz;
	end_etl_t timestamptz;
BEGIN 
	SELECT clock_timestamp() INTO start_etl_t;
	LOOP
        EXIT WHEN clock_timestamp() >= start_etl_t + interval '2 seconds';
   	END LOOP;

	SELECT clock_timestamp() INTO end_etl_t;

--=================== загрзука  ft_balance_f  =============================================================--
	MERGE INTO ds.ft_balance_f AS main
	USING stage.ft_balance_f AS stg
		ON main.on_date = stg.on_date::date AND main.account_rk = stg.account_rk
	WHEN MATCHED THEN
		UPDATE SET 
			currency_rk = stg.currency_rk
			,balance_out = stg.balance_out::float
	WHEN NOT MATCHED THEN 
		INSERT (on_date, account_rk, currency_rk, balance_out)
		VALUES (stg.on_date::date, stg.account_rk, stg.currency_rk, stg.balance_out);

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'ft_balance_f', 'ds', 'SUCCES', end_etl_t);	

	DROP TABLE stage.ft_balance_f;


--=================== загрзука  ft_balance_f  =============================================================--
	
	TRUNCATE TABLE ds.ft_posting_f;

	INSERT INTO ds.ft_posting_f (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
	SELECT oper_date::date, credit_account_rk::bigint, debet_account_rk::bigint, credit_amount::float
			, debet_amount::float FROM stage.ft_posting_f;
		
	DROP TABLE stage.ft_posting_f;

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'ft_posting_f', 'ds', 'SUCCES', end_etl_t);	

--=================== загрзука  md_account_d =============================================================--
	MERGE INTO ds.md_account_d AS main
	USING stage.md_account_d AS stg
		ON main.data_actual_date = stg.data_actual_date::date AND main.account_rk = stg.account_rk::bigint
	WHEN MATCHED THEN
		UPDATE SET 
			 data_actual_end_date = stg.data_actual_end_date::date
			,account_number = stg.account_number::varchar(20)
			,char_type = stg.char_type::varchar(1)
			,currency_rk= stg.currency_rk::bigint
			,currency_code = stg.currency_code::varchar(3)
	WHEN NOT MATCHED THEN 
		INSERT (data_actual_date, data_actual_end_date, account_rk
							,account_number, char_type, currency_rk, currency_code)
		VALUES (stg.data_actual_date::date, stg.data_actual_end_date::date, stg.account_rk::bigint
		, stg.account_number::varchar(20), stg.char_type::varchar(1), stg.currency_rk::bigint
		, stg.currency_code::varchar(3));

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'md_account_d', 'ds', 'SUCCES', end_etl_t);	

	DROP TABLE stage.md_account_d;

--=================== загрзука md_currency_d =============================================================--
	MERGE INTO ds.md_currency_d AS main
	USING stage.md_currency_d AS stg
		ON main.data_actual_date = stg.data_actual_date::date 
	AND main.currency_rk = stg.currency_rk::bigint
	WHEN MATCHED THEN
		UPDATE SET 
			 data_actual_end_date = stg.data_actual_end_date::date
			,code_iso_char = stg.code_iso_char::varchar(3)
			,currency_code = stg.currency_code::varchar(3)
	WHEN NOT MATCHED THEN 
		INSERT (data_actual_date, currency_rk, data_actual_end_date, code_iso_char ,currency_code)
		VALUES (stg.data_actual_date::date, stg.currency_rk::bigint
				, stg.data_actual_end_date::date, stg.code_iso_char::varchar(3), stg.currency_code::varchar(3));

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'md_currency_d', 'ds', 'SUCCES', end_etl_t);

	DROP TABLE stage.md_currency_d;

--=================== загрузка md_exchange_rate_d=============================================================--
	MERGE INTO ds.md_exchange_rate_d AS main
	USING stage.md_exchange_rate_d AS stg
		ON main.data_actual_date = stg.data_actual_date::date 
	AND main.currency_rk = stg.currency_rk::bigint
	WHEN MATCHED THEN
		UPDATE SET 
			 data_actual_end_date = stg.data_actual_end_date::date
			,code_iso_num = stg.code_iso_num::varchar(3)
			,reduced_cource = stg.reduced_cource::float
	WHEN NOT MATCHED THEN 
		INSERT (data_actual_date, currency_rk, data_actual_end_date, code_iso_num, reduced_cource)
		VALUES (stg.data_actual_date::date, stg.currency_rk::bigint
				, stg.data_actual_end_date::date, stg.code_iso_num::varchar(3), stg.reduced_cource::float);

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'md_exchange_rate_d', 'ds', 'SUCCES', end_etl_t);

	DROP TABLE stage.md_exchange_rate_d;

--=================== загрзука md_ledger_account_s =============================================================--
	MERGE INTO ds.md_ledger_account_s AS main
	USING stage.md_ledger_account_s AS stg
		ON main.ledger_account = stg.ledger_account::int4 
	AND main.start_date = stg.start_date::date
	WHEN MATCHED THEN
		UPDATE SET 
			chapter_name = stg.chapter_name::varchar(16),
			section_number = stg.section_number::int4,
			section_name = stg.section_name::varchar(22),
			subsection_name = stg.subsection_name::varchar(21),
			ledger1_account = stg.ledger1_account::int4,
			ledger1_account_name = stg.ledger1_account_name::varchar(47),
			ledger_account = stg.ledger_account::int4,
			ledger_account_name = stg.ledger_account_name::varchar(154),
			characteristic = stg.characteristic::bpchar(1),
			start_date = stg.start_date::date,
			end_date = stg.end_date::date
	WHEN NOT MATCHED THEN 
		INSERT (chapter_name, section_number, section_name
			, subsection_name, ledger1_account, ledger1_account_name
			, ledger_account, ledger_account_name, characteristic
			, start_date, end_date)
		VALUES (chapter_name::varchar(16), section_number::int4, section_name::varchar(22)
			, subsection_name::varchar(21), ledger1_account::int4, ledger1_account_name::varchar(47)
			, ledger_account::int4, ledger_account_name::varchar(154), characteristic::bpchar(1)
			, start_date::date, end_date::date);

	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, end_etl)
	VALUES (start_etl_t, 'md_ledger_account_s', 'ds', 'SUCCES', end_etl_t);

	DROP TABLE stage.md_ledger_account_s;

	EXCEPTION
		WHEN OTHERS THEN
	INSERT INTO logs.log_stg (start_etl, status, message, end_etl)
	VALUES (start_etl_t,'FAILURE',SQLERRM, end_etl_t);
END;
$$;


CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_t(i_ondate date) -- процедура по оборотам счетов
LANGUAGE plpgsql 
AS $$
DECLARE 
	start_etl_t timestamptz;
	end_etl_t timestamptz;
BEGIN 
	SELECT clock_timestamp() INTO start_etl_t;

DROP TABLE IF EXISTS turnover_stg;

CREATE TEMP TABLE turnover_stg ( --времянка для последующего merge
on_date date
, account_rk bigint
, credit_amount decimal(23,8)
, credit_amount_rub decimal(23,8)
, debet_amount decimal(23,8)
, debet_amount_rub decimal(23,8));

INSERT INTO turnover_stg(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
	WITH posting AS (
					SELECT oper_date, credit_account_rk AS account_rk, credit_amount, 0 debet_amount
				FROM ds.ft_posting_f
				WHERE oper_date = i_ondate
					UNION ALL
				SELECT oper_date, debet_account_rk account_rk, 0 credit_amount, debet_amount
				FROM ds.ft_posting_f
				WHERE oper_date = i_ondate)
	SELECT 
	--account_number
	i_ondate
	, acc.account_rk
	--, char_type 
	--,code_iso_char
	--, reduced_cource
	, CAST(sum(credit_amount) AS decimal(23,8)) credit_amount
	, CAST(sum(CAST(credit_amount * coalesce(reduced_cource, 1) AS decimal(23,8))) AS decimal(23,8)) credit_amount_rub
	, CAST(sum(debet_amount) AS decimal(23,8)) debet_amount
	, CAST(sum(CAST(debet_amount * coalesce(reduced_cource, 1)  AS decimal(23,8))) AS decimal(23,8)) debet_amount_rub
	FROM posting post
	JOIN ds.md_account_d acc 
		ON post.account_rk = acc.account_rk
		AND post.oper_date BETWEEN acc.data_actual_date AND acc.data_actual_end_date
	JOIN ds.md_currency_d curr 
		ON acc.currency_rk = curr.currency_rk
		AND post.oper_date BETWEEN curr.data_actual_date AND curr.data_actual_end_date 
	LEFT JOIN ds.md_exchange_rate_d exc_curr
		ON exc_curr.currency_rk = curr.currency_rk
		AND post.oper_date BETWEEN exc_curr.data_actual_date AND exc_curr.data_actual_end_date 
	GROUP BY 1, 2;
-- делаю мердж для того, что бы данные обновлялись при одинаковых улючах (дата и счет), а не добовлялись.
MERGE INTO dm.account_turnover_f AS main 
USING turnover_stg as stg 
ON main.on_date = stg.on_date AND main.account_rk = stg.account_rk
WHEN MATCHED THEN
		UPDATE SET 
		credit_amount = stg.credit_amount
		, credit_amount_rub = stg.credit_amount_rub
		, debet_amount = stg.debet_amount
		, debet_amount_rub = stg.debet_amount_rub
WHEN NOT MATCHED THEN 
	INSERT (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
	VALUES (stg.on_date, stg.account_rk, stg.credit_amount, stg.credit_amount_rub, stg.debet_amount, stg.debet_amount_rub);

--время окончания ETL
SELECT clock_timestamp() INTO end_etl_t;
-- логирую
INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, message,  end_etl)
		VALUES (start_etl_t, 'account_turnover_f', 'dm', 'SUCCES', i_ondate::text , end_etl_t);
--ошибки для логов
EXCEPTION
			WHEN OTHERS THEN
		SELECT clock_timestamp() INTO end_etl_t;
		INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, message, end_etl)
		VALUES (start_etl_t, 'account_turnover' , 'dm','FAILURE',SQLERRM, end_etl_t);
END ;
$$;


CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_t_january() -- что бы не расчитывать на каждый день обороты, сделал процедуру
LANGUAGE plpgsql
AS $$ 
DECLARE i_ondate date;
BEGIN 
	FOR i_ondate IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date,'1 day'::INTERVAL) LOOP
		CALL dm.fill_account_turnover_t(i_ondate::date);
	END LOOP;
END;
$$;


CREATE OR REPLACE PROCEDURE dm.balance_test(i_ondate date) --процедура по пересчету остатка на требуемый день
LANGUAGE plpgsql
AS $$ 
DECLARE 
	start_etl_t timestamptz;
	end_etl_t timestamptz;
BEGIN 
	SELECT clock_timestamp() INTO start_etl_t;

	DROP TABLE IF EXISTS stage_balance_etl;
--делаю времянку для мерджа
    CREATE TEMP TABLE stage_balance_etl AS (
	WITH max_date AS ( --ищу последнюю дату баланса
					SELECT account_rk
							, i_ondate i_on_date
							, max(on_date) on_date 
					FROM dm.account_balance_f 
					WHERE on_date < i_ondate -- по условию, что берется баланс за преддущий день
					GROUP BY 1,2)
	, stg_balance AS (
					SELECT i_on_date
					, bal.on_date
					, bal.account_rk
					, balance_out
					, char_type
					, cur.currency_rk
					, rate.reduced_cource
					, tur.on_date
					, COALESCE (credit_amount,0) credit_amount
					, COALESCE (debet_amount,0) debet_amount
					FROM dm.account_balance_f bal 
					JOIN max_date m 
						USING (account_rk, on_date)
					LEFT JOIN dm.account_turnover_f tur
						ON tur.account_rk = bal.account_rk
						AND tur.on_date = m.i_on_date
					JOIN ds.md_account_d acc 
						ON bal.account_rk = acc.account_rk
					JOIN ds.md_currency_d cur
						ON cur.currency_rk = acc.currency_rk 
						AND bal.on_date BETWEEN cur.data_actual_date AND cur.data_actual_end_date
					LEFT JOIN ds.md_exchange_rate_d rate 
						ON cur.currency_rk = rate.currency_rk
						AND bal.on_date BETWEEN rate.data_actual_date AND rate.data_actual_end_date)
	SELECT i_on_date, account_rk ,
		CASE 
			WHEN char_type = 'А' 
				THEN balance_out + debet_amount - credit_amount
			else 
				balance_out - debet_amount + credit_amount
		END balance
		,CASE 
			WHEN char_type = 'А' 
				THEN (balance_out + debet_amount - credit_amount) * COALESCE(reduced_cource, 1)
			else 
				(balance_out - debet_amount + credit_amount) * COALESCE(reduced_cource, 1)
		END balance_out_rub
	FROM stg_balance);

MERGE INTO dm.account_balance_f main --делаю мердж для того,что бы при повторном накате данные обновлялись.
	USING stage_balance_etl stg
	ON i_on_date = on_date
	AND main.account_rk = stg.account_rk
WHEN MATCHED THEN 
	UPDATE SET
		balance_out = stg.balance
		, balance_out_rub = stg.balance_out_rub
	WHEN NOT MATCHED THEN 
		INSERT (on_date, account_rk, balance_out, balance_out_rub)
		VALUES (stg.i_on_date, stg.account_rk, stg.balance, stg.balance_out_rub);

	SELECT clock_timestamp() INTO end_etl_t; --время окончания ETL
--Логирую
	INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, message,  end_etl)
		VALUES (start_etl_t, 'account_balance_f', 'dm', 'SUCCES', i_ondate::text , end_etl_t);
--ошибка
EXCEPTION
			WHEN OTHERS THEN
		SELECT clock_timestamp() INTO end_etl_t;
	
		INSERT INTO logs.log_stg (start_etl, table_name , schema_name , status, message, end_etl)
		VALUES (start_etl_t, 'account_balance_f' , 'dm','FAILURE',SQLERRM, end_etl_t);
END; 
$$;


CREATE OR REPLACE PROCEDURE dm.fill_account_balance_january() -- аналогично по остаткам как и с оборотами.
LANGUAGE plpgsql
AS $$ 
DECLARE i_ondate date;
BEGIN 
	FOR i_ondate IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date,'1 day'::INTERVAL) LOOP
		CALL dm.balance_test(i_ondate::date);
	END LOOP;
END;
$$;
