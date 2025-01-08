
CREATE TABLE dm.account_balance_f AS ( -- созданю таблицу по остаткам
	SELECT 
	on_date
	, acc.account_rk
	, bal.balance_out
--	, acc.char_type
--	, acc.currency_rk
	, balance_out * coalesce (reduced_cource, 1)::float balance_out_rub
	FROM ds.ft_balance_f bal
	JOIN ds.md_account_d acc 
		ON bal.account_rk = acc.account_rk
	JOIN ds.md_currency_d cur
		ON cur.currency_rk = acc.currency_rk 
		AND on_date BETWEEN cur.data_actual_date AND cur.data_actual_end_date
	LEFT JOIN ds.md_exchange_rate_d rate 
		ON cur.currency_rk = rate.currency_rk
		AND on_date BETWEEN rate.data_actual_date AND rate.data_actual_end_date);
		
CALL  dm.fill_account_balance_january();

SELECT * FROM dm.account_balance_f