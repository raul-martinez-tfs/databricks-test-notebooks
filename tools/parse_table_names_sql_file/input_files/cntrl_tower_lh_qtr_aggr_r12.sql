-- /**************************************************************************
-- Artefact Name :- lh_qtr_aggr control tower for Lighthouse 
-- Description :-   lh_qtr_aggr control tower for Lighthouse
-- -------------------------------------------------------------------------------------------------------------------------------
-- Change Log
-- Version :        Date :                                Description                                     Changed By
-- -------------------------------------------------------------------------------------------------------------------------------
-- 0.0            	  04-01-2022                  			 First draft of sql file    			       Vijay Kelkar
-- 1.0				  06-04-2022                             Adding rec_crt_ts,rec_updt_ts
--																Columns                                     Vijay Kelkar
-- **************************************************************************/

-- Databricks notebook source
(
Select
	COALESCE (ioh_sku_site_cd,
	purchase_sku_site_cd,
	recpt_sku_site_cd,
	usage_sku_site_cd,
	erp_sku_site_cd,
	fcf_sku_site_cd,
	fg_sku_site_cd,
	aop_sku_site_cd,
	fes_sku_site_cd) as sku_site_cd ,
	COALESCE (ioh_yearqtrnbr,
	purchase_yearqtrnbr,
	recpt_yearqtrnbr,
	usage_yearqtrnbr,
	erp_yearqtrnbr,
	fcf_yearqtrnbr,
	fg_yearqtrnbr,
	aop_yearqtrnbr,
	fes_yearqtrnbr) as yearqtrnbr,
	ioh_qty,
	ioh_pmar_amt,
	purchase_po_qty,
	purchase_Po_pmar_amt,
	recpt_qty,
	recpt_pmar_amt,
	usage_qty,
	usage_pmar_amt,
	erp_qty,
	erp_Pmar_amt,
	fcf_qty,
	fcf_Pmar_amt,
	fg_qty,
	fg_Pmar_amt,
	aop_qty,
	aop_Pmar_amt,
	fes_qty,
	fes_Pmar_amt,
	COALESCE (Prior_purchase_yearqtrnbr,
	Prior_recpt_yearqtrnbr,
	Prior_usage_yearqtrnbr,
	Prior_erp_yearqtrnbr,
	Prior_fcf_yearqtrnbr,
	Prior_fg_yearqtrnbr,
	Prior_aop_yearqtrnbr,
	Prior_fes_yearqtrnbr) as Prior_yearqtrnbr,
	Prior_purchase_po_qty,
	Prior_purchase_Po_pmar_amt,
	Prior_recpt_po_qty,
	Prior_recpt_po_pmar_amt,
	Prior_usage_qty,
	Prior_usage_pmar_amt ,
	Prior_erp_qty,
	Prior_erp_Pmar_amt,
	Prior_fcf_qty,
	Prior_fcf_Pmar_amt,
	Prior_fg_qty,
	Prior_fg_Pmar_amt Prior_aop_qty,
	Prior_aop_Pmar_amt,
	Prior_fes_qty,
	Prior_fes_Pmar_amt,
	'ebs_lgn' as src_sys_cd
	,cast(current_timestamp as string) as rec_crt_ts
    ,cast(current_timestamp as string) as rec_updt_ts
 
from
	(
	SELECT
		distinct trim(finv.item_nbr)|| '|' || dpro.prim_site_cd as ioh_sku_site_cd,
		cast (fscl_yr_qtr_nbr as string) as ioh_yearqtrnbr,
		SUM(finv.on_hand_qty) as ioh_qty,
		SUM((finv.unit_cost_co_amt * finv.on_hand_qty)) as ioh_pmar_amt
	FROM
        f_invntry_bal_dly_hist finv
	LEFT JOIN d_product dpro ON
		(finv.item_nbr = substring(dpro.sku, 1, (instr(dpro.sku, '|') - 1))
		AND (dpro.src_sys_cd = 'ebs_lgn'))
	left join (
		SELECT
			distinct cal_yr_mth_nbr ,
			fscl_yr_qtr_nbr
		FROM
			d_date
		where
			fscl_yr_nbr between 2020 and 2023) QtrConv on
		cast(substring (finv.capture_dt, 1, 6) as string)= QtrConv.cal_yr_mth_nbr
	WHERE
		finv.src_sys_cd IN ('ebs_lgn')
		and ( finv.item_nbr is not null
		or finv.plant_cd is not null)
		and finv.capture_dt in (
		select
			max(capture_dt)
		from
			f_invntry_bal_dly_hist)
	group by
		trim(finv.item_nbr)|| '|' || dpro.prim_site_cd,
		fscl_yr_qtr_nbr
union all
	SELECT
		distinct trim(finv.item_nbr)|| '|' || 'M2M' as ioh_sku_site_cd,
		cast (fscl_yr_qtr_nbr as string) as ioh_yearqtrnbr,
		SUM(finv.on_hand_qty) as ioh_qty,
		SUM((finv.unit_cost_co_amt * finv.on_hand_qty)) as ioh_pmar_amt
	FROM
		f_invntry_bal_dly_hist finv
	LEFT JOIN d_product dpro ON
		(finv.item_nbr = dpro.sku
		AND (dpro.src_sys_cd = 'ebs_lgn'))
	left join (
		SELECT
			distinct cal_yr_mth_nbr ,
			fscl_yr_qtr_nbr
		FROM
			d_date
		where
			fscl_yr_nbr between 2020 and 2023) QtrConv on
		cast(substring (finv.capture_dt, 1, 6) as string)= QtrConv.cal_yr_mth_nbr
	WHERE
		finv.src_sys_cd IN ('ebs_lgn')
		and ( finv.item_nbr is not null
		or finv.plant_cd is not null)
		and finv.capture_dt in (
		select
			max(capture_dt)
		from
			f_invntry_bal_dly_hist)
	group by
		trim(finv.item_nbr)|| '|' || 'M2M',
		fscl_yr_qtr_nbr ) ioh
full outer join (
	select
		purchase_sku_site_cd,
		purchase_yearqtrnbr,
		purchase_po_qty,
		purchase_Po_pmar_amt,
		cast(lag(cast(purchase_yearqtrnbr as bigint)) over (partition by purchase_sku_site_cd order by cast(purchase_yearqtrnbr as int) asc ) as string) as Prior_purchase_yearqtrnbr,
		cast(lag(cast(purchase_po_qty as bigint)) over (partition by purchase_sku_site_cd order by cast(purchase_yearqtrnbr as int) asc ) as string) as Prior_purchase_po_qty,
		cast(lag(cast(purchase_Po_pmar_amt as bigint)) over (partition by purchase_sku_site_cd order by cast(purchase_yearqtrnbr as int) asc ) as string) as Prior_purchase_Po_pmar_amt
	from
		(
		SELECT
			trim(item_nbr)|| '|' || trim(case when src_sys_cd like '%m2m%' then 'M2M' else site_id end) as purchase_sku_site_cd,
			cast (fscl_yr_qtr_nbr as string) as purchase_yearqtrnbr,
			sum(open_qty) as purchase_po_qty,
			sum(ext_prc_co_pmar_amt) as purchase_Po_pmar_amt
		FROM
			f_purchase_order
		left outer join (
			SELECT
				distinct cal_yr_mth_nbr ,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2020 and 2023) QtrConv on
			cast(substring(plan_delvry_dt, 1, 6) as string) = QtrConv.cal_yr_mth_nbr
		WHERE
			cast (substring(plan_delvry_dt, 1, 6) as string) BETWEEN '202101' AND '202301'
			and item_nbr is not null
			and src_sys_cd in ('ebs_lgn')
			and item_status_cd <> '%CLOSED%'
		GROUP BY
			trim(item_nbr)|| '|' || trim(case when src_sys_cd like '%m2m%' then 'M2M' else site_id end),
			fscl_yr_qtr_nbr)
	order by
		purchase_sku_site_cd,
		purchase_yearqtrnbr ) purchase on
	purchase.purchase_sku_site_cd = ioh.ioh_sku_site_cd
	and purchase.purchase_yearqtrnbr = ioh.ioh_yearqtrnbr
full outer join (
	select
		recpt_sku_site_cd,
		recpt_yearqtrnbr,
		recpt_qty,
		recpt_pmar_amt,
		cast(lag(cast(recpt_yearqtrnbr as bigint)) over (partition by recpt_sku_site_cd order by cast(recpt_yearqtrnbr as int) asc ) as string) as Prior_recpt_yearqtrnbr,
		cast(lag(cast(recpt_qty as bigint)) over (partition by recpt_sku_site_cd order by cast(recpt_yearqtrnbr as int) asc ) as string) as Prior_recpt_po_qty,
		cast(lag(cast(recpt_pmar_amt as bigint)) over (partition by recpt_sku_site_cd order by cast(recpt_yearqtrnbr as int) asc ) as string) as Prior_recpt_Po_pmar_amt
	from
		(
		SELECT
			trim(item_nbr)|| '|' || trim(case when src_sys_cd like '%m2m%' then 'M2M' else site_cd end) as recpt_sku_site_cd,
			cast (fscl_yr_qtr_nbr as string) as recpt_yearqtrnbr,
			sum(recpt_txn_qty) as recpt_qty,
			sum(recpt_pmar_amt) as recpt_pmar_amt
		FROM
			f_po_receipt
		left outer join (
			SELECT
				distinct cal_yr_mth_nbr ,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2020 and 2023) QtrConv on
			recpt_date_period = QtrConv.cal_yr_mth_nbr
		WHERE
			recpt_date_period BETWEEN '202101' AND '202301'
			and item_nbr is not null
			and src_sys_cd in ('ebs_lgn')
		GROUP BY
			trim(item_nbr)|| '|' || trim(case when src_sys_cd like '%m2m%' then 'M2M' else site_cd end),
			fscl_yr_qtr_nbr )
	order by
		recpt_sku_site_cd,
		recpt_yearqtrnbr ) receipt on
	purchase.purchase_sku_site_cd = receipt.recpt_sku_site_cd
	and purchase.purchase_yearqtrnbr = receipt.recpt_yearqtrnbr
full outer join (
	select
		usage_sku_site_cd,
		usage_yearqtrnbr,
		usage_qty,
		usage_pmar_amt,
		cast(lag(cast(usage_yearqtrnbr as bigint)) over (partition by usage_sku_site_cd order by cast(usage_yearqtrnbr as int) asc ) as string) as Prior_usage_yearqtrnbr,
		cast(lag(cast(usage_qty as bigint)) over (partition by usage_sku_site_cd order by cast(usage_yearqtrnbr as int) asc ) as string) as Prior_usage_qty,
		cast(lag(cast(usage_pmar_amt as bigint)) over (partition by usage_sku_site_cd order by cast(usage_yearqtrnbr as int) asc ) as string) as Prior_usage_pmar_amt
	from
		(
		SELECT
			DISTINCT trim(fit.item_nbr)|| '|' ||
			(case
				when src_sys_cd = 'ebs_lgn' then fit.plant_cd
				else 'M2M'
			end ) as usage_sku_site_cd,
			cast (fscl_yr_qtr_nbr as string) as usage_yearqtrnbr,
			SUM(fit.txn_qty*(-1)) as usage_qty,
			Sum((fit.txn_qty * (-1)* prod_cost)) as usage_pmar_amt
		FROM
			f_invntry_txn fit
		left outer join (
			SELECT
				distinct cal_yr_mth_nbr ,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2020 and 2023) QtrConv on
			fit.txn_period = QtrConv.cal_yr_mth_nbr
		WHERE
			fit.txn_period BETWEEN '202101' AND '202301'
			and src_sys_cd in ('ebs_lgn')
			and ((fit.txn_type_cd in ('33', '35', '62'))
			or (fit.txn_type_cd = '32'
			and fit.rsn_nm in ('8310', '9050', '9018'))
			or (fit.txn_type_cd = '63'
			and fit.rsn_nm in ('9050')))
			and fit.item_nbr is not null
		GROUP BY
			trim(fit.item_nbr)|| '|' ||
			(case
				when src_sys_cd = 'ebs_lgn' then fit.plant_cd
				else 'M2M'
			end ),
			QtrConv.fscl_yr_qtr_nbr)
	order by
		usage_sku_site_cd,
		usage_yearqtrnbr ) usage on
	usage.usage_sku_site_cd = receipt.recpt_sku_site_cd
	and usage.usage_yearqtrnbr = receipt.recpt_yearqtrnbr
full outer join (
	select
		erp_sku_site_cd,
		erp_yearqtrnbr,
		erp_qty,
		erp_Pmar_amt,
		cast(lag(cast(erp_yearqtrnbr as bigint)) over (partition by erp_sku_site_cd order by cast(erp_yearqtrnbr as int) asc ) as string) as Prior_erp_yearqtrnbr,
		cast(lag(cast(erp_qty as string)) over (partition by erp_sku_site_cd order by cast(erp_yearqtrnbr as int) asc ) as string) as Prior_erp_qty,
		cast(lag(cast(erp_Pmar_amt as string)) over (partition by erp_sku_site_cd order by cast(erp_yearqtrnbr as int) asc ) as string) as Prior_erp_pmar_amt
	from
		(
		SELECT
			distinct concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)) as erp_sku_site_cd,
			cast(MthConv.fscl_yr_qtr_nbr as string) as erp_yearqtrnbr,
			sum(sup_dmd_table.qty) as erp_qty,
			sum(sup_dmd_table.pmar_amt) as erp_Pmar_amt
		from
			f_forecast sup_dmd_table
		left outer join (
			SELECT
				distinct concat(cast(fscl_yr_nbr as string), cast(fscl_wk_nbr as string)) as fscl_wk_nbr,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2021 and 2023) MthConv on
			sup_dmd_table.week_nbr = MthConv.fscl_wk_nbr
		where
			sup_dmd_table.forecast_type = 'ERP Demand'
			and sup_dmd_table.capture_dt = (
			select
				max(sup_dmd_table.capture_dt)
			from
				f_forecast sup_dmd_table)
		group by
			concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)),
			MthConv.fscl_yr_qtr_nbr )
	order by
		erp_sku_site_cd,
		erp_yearqtrnbr ) erp ON
	usage.usage_sku_site_cd = erp.erp_sku_site_cd
	and usage.usage_yearqtrnbr = erp.erp_yearqtrnbr
full outer join (
	select
		fg_sku_site_cd,
		fg_yearqtrnbr,
		fg_qty,
		fg_Pmar_amt,
		cast(lag(cast(fg_yearqtrnbr as bigint)) over (partition by fg_sku_site_cd order by cast(fg_yearqtrnbr as int) asc ) as string) as Prior_fg_yearqtrnbr,
		cast(lag(cast(fg_qty as string)) over (partition by fg_sku_site_cd order by cast(fg_yearqtrnbr as int) asc ) as string) as Prior_fg_qty,
		cast(lag(cast(fg_Pmar_amt as string)) over (partition by fg_sku_site_cd order by cast(fg_yearqtrnbr as int) asc ) as string) as Prior_fg_pmar_amt
	from
		(
		SELECT
			distinct concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)) as fg_sku_site_cd,
			cast(MthConv.fscl_yr_qtr_nbr as string) as fg_yearqtrnbr,
			sum(sup_dmd_table.qty) as fg_qty,
			sum(sup_dmd_table.pmar_amt) as fg_Pmar_amt
		from
			f_forecast sup_dmd_table
		left outer join (
			SELECT
				distinct concat(cast(fscl_yr_nbr as string), cast(fscl_wk_nbr as string)) as fscl_wk_nbr,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2021 and 2023) MthConv on
			sup_dmd_table.week_nbr = MthConv.fscl_wk_nbr
		where
			sup_dmd_table.forecast_type = 'Forced Growth'
			and sup_dmd_table.capture_dt = (
			select
				max(sup_dmd_table.capture_dt)
			from
				f_forecast sup_dmd_table)
		group by
			concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)),
			MthConv.fscl_yr_qtr_nbr )
	order by
		fg_sku_site_cd,
		fg_yearqtrnbr ) fg ON
	fg.fg_sku_site_cd = erp.erp_sku_site_cd
	and fg.fg_yearqtrnbr = erp.erp_yearqtrnbr
full outer join (
	select
		aop_sku_site_cd,
		aop_yearqtrnbr,
		aop_qty,
		aop_Pmar_amt,
		cast(lag(cast(aop_yearqtrnbr as bigint)) over (partition by aop_sku_site_cd order by cast(aop_yearqtrnbr as int) asc ) as string) as Prior_aop_yearqtrnbr,
		cast(lag(cast(aop_qty as string)) over (partition by aop_sku_site_cd order by cast(aop_yearqtrnbr as int) asc ) as string) as Prior_aop_qty,
		cast(lag(cast(aop_Pmar_amt as string)) over (partition by aop_sku_site_cd order by cast(aop_yearqtrnbr as int) asc ) as string) as Prior_aop_pmar_amt
	from
		(
		SELECT
			distinct concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)) as aop_sku_site_cd,
			cast(MthConv.fscl_yr_qtr_nbr as string) as aop_yearqtrnbr,
			sum(sup_dmd_table.qty) as aop_qty,
			sum(sup_dmd_table.pmar_amt) as aop_Pmar_amt
		from
			f_forecast sup_dmd_table
		left outer join (
			SELECT
				distinct concat(cast(fscl_yr_nbr as string), cast(fscl_wk_nbr as string)) as fscl_wk_nbr,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2021 and 2023) MthConv on
			sup_dmd_table.week_nbr = MthConv.fscl_wk_nbr
		where
			sup_dmd_table.forecast_type = 'Firm Requirements'
			and sup_dmd_table.capture_dt = (
			select
				max(sup_dmd_table.capture_dt)
			from
				f_forecast sup_dmd_table)
		group by
			concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)),
			MthConv.fscl_yr_qtr_nbr )
	order by
		aop_sku_site_cd,
		aop_yearqtrnbr ) aop ON
	fg.fg_sku_site_cd = aop.aop_sku_site_cd
	and fg.fg_yearqtrnbr = aop.aop_yearqtrnbr
full outer join (
	select
		fes_sku_site_cd,
		fes_yearqtrnbr,
		fes_qty,
		fes_Pmar_amt,
		cast(lag(cast(fes_yearqtrnbr as bigint)) over (partition by fes_sku_site_cd order by cast(fes_yearqtrnbr as int) asc ) as string) as Prior_fes_yearqtrnbr,
		cast(lag(cast(fes_qty as string)) over (partition by fes_sku_site_cd order by cast(fes_yearqtrnbr as int) asc ) as string) as Prior_fes_qty,
		cast(lag(cast(fes_Pmar_amt as string)) over (partition by fes_sku_site_cd order by cast(fes_yearqtrnbr as int) asc ) as string) as Prior_fes_pmar_amt
	from
		(
		SELECT
			distinct concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)) as fes_sku_site_cd,
			cast(MthConv.fscl_yr_qtr_nbr as string) as fes_yearqtrnbr,
			sum(sup_dmd_table.qty) as fes_qty,
			sum(sup_dmd_table.pmar_amt) as fes_Pmar_amt
		from
			f_forecast sup_dmd_table
		left outer join (
			SELECT
				distinct concat(cast(fscl_yr_nbr as string), cast(fscl_wk_nbr as string)) as fscl_wk_nbr,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2021 and 2023) MthConv on
			sup_dmd_table.week_nbr = MthConv.fscl_wk_nbr
		where
			sup_dmd_table.forecast_type = 'Forcast ERP Simulation'
			and sup_dmd_table.capture_dt = (
			select
				max(sup_dmd_table.capture_dt)
			from
				f_forecast sup_dmd_table)
		group by
			concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)),
			MthConv.fscl_yr_qtr_nbr )
	order by
		fes_sku_site_cd,
		fes_yearqtrnbr ) fes ON
	fes.fes_sku_site_cd = aop.aop_sku_site_cd
	and fes.fes_yearqtrnbr = aop.aop_yearqtrnbr
full outer join (
	select
		fcf_sku_site_cd,
		fcf_yearqtrnbr,
		fcf_qty,
		fcf_Pmar_amt,
		cast(lag(cast(fcf_yearqtrnbr as bigint)) over (partition by fcf_sku_site_cd order by cast(fcf_yearqtrnbr as int) asc ) as string) as Prior_fcf_yearqtrnbr,
		cast(lag(cast(fcf_qty as string)) over (partition by fcf_sku_site_cd order by cast(fcf_yearqtrnbr as int) asc ) as string) as Prior_fcf_qty,
		cast(lag(cast(fcf_Pmar_amt as string)) over (partition by fcf_sku_site_cd order by cast(fcf_yearqtrnbr as int) asc ) as string) as Prior_fcf_pmar_amt
	from
		(
		SELECT
			distinct concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)) as fcf_sku_site_cd,
			cast(MthConv.fscl_yr_qtr_nbr as string) as fcf_yearqtrnbr,
			sum(sup_dmd_table.qty)* 1.15 as fcf_qty,
			sum(sup_dmd_table.pmar_amt)* 1.15 as fcf_Pmar_amt
		from
			f_forecast sup_dmd_table
		left outer join (
			SELECT
				distinct concat(cast(fscl_yr_nbr as string), cast(fscl_wk_nbr as string)) as fscl_wk_nbr,
				fscl_yr_qtr_nbr
			FROM
				d_date
			where
				fscl_yr_nbr between 2021 and 2023) MthConv on
			sup_dmd_table.week_nbr = MthConv.fscl_wk_nbr
		where
			sup_dmd_table.forecast_type = 'ERP Demand'
			and sup_dmd_table.capture_dt = (
			select
				max(sup_dmd_table.capture_dt)
			from
				f_forecast sup_dmd_table)
		group by
			concat(regexp_replace(sup_dmd_table.item_nbr, '[$]',""), concat('|', case when sup_dmd_table.src_sys_cd like '%m2m%' then 'M2M' else plant_cd end)),
			MthConv.fscl_yr_qtr_nbr )
	order by
		fcf_sku_site_cd,
		fcf_yearqtrnbr ) fcf on
	fcf.fcf_sku_site_cd = fes.fes_sku_site_cd
	and fcf.fcf_yearqtrnbr = fes.fes_yearqtrnbr
	and fcf.fcf_sku_site_cd = ioh.ioh_sku_site_cd
	and fcf.fcf_yearqtrnbr = ioh.ioh_yearqtrnbr )
