import pandas as pd

def sales_diff_qty(conn,O_status,I_hier1,I_hier2,I_hier3,I_sdate,I_year_interval,I_weeks):
	L_func = "sales_diff_qty"
	try:
		Q_sel_qty = """ WITH RECURSIVE Levelcte AS( 
		SELECT 1 AS level
				 UNION ALL
				SELECT level + 1
				  FROM Levelcte
						 WHERE level < IFNULL(NULLIF(IFNULL(%s, 0), 0), 1)), #p_year_interval
			date_intervals AS (SELECT
								DATE_SUB(
										STR_TO_DATE(IFNULL(%s, CURDATE()), '%%Y-%%m-%%d'), #p_sdate
										INTERVAL level YEAR) AS start_date,
							DATE_ADD(
								DATE_SUB(
										STR_TO_DATE(IFNULL(%s, CURDATE()), '%%Y-%%m-%%d'), #p_sdate
										INTERVAL level YEAR),
										INTERVAL 7 * IFNULL(NULLIF(IFNULL(%s, 0), 0), 2) DAY) AS end_date #p_weeks
							 FROM Levelcte),
			item_loc_yr_units AS 
			            (SELECT
							   tdh.item,
							   tdh.location,
							   SUM(tdh.qty) AS units_sold,
							   EXTRACT(YEAR FROM STR_TO_DATE(IFNULL(%s, CURDATE()), '%%Y-%%m-%%d')) - EXTRACT(YEAR FROM STR_TO_DATE(tdh.trn_date, '%%Y-%%m-%%d')) AS year_difference,  #p_sdate
							   trn_date
							FROM
			                    trn_data_history tdh
			                    JOIN date_intervals di ON tdh.trn_date BETWEEN di.start_date AND di.end_date
			                WHERE
			                    ((tdh.hier1 = %s OR %s IS NULL)
			                    OR (tdh.hier1 IS NULL AND %s IS NULL))
			                    AND 
			                    ((tdh.hier2 = %s OR %s IS NULL)
			                    OR (tdh.hier2 IS NULL AND %s IS NULL))
			                    AND
			                    ((tdh.hier3 = %s OR %s IS NULL)
			                    OR (tdh.hier3 IS NULL AND %s IS NULL))
			                    AND tdh.trn_type = 'SLS'
			                GROUP BY
			                    tdh.item,
			                    tdh.location,
			                    qty,
			                    EXTRACT(YEAR FROM STR_TO_DATE(IFNULL(%s, CURDATE()), 'DD-MM-YY')) - EXTRACT(YEAR FROM STR_TO_DATE(trn_date, 'DD-MM-YY')),  #p_sdate
			                    trn_date),
			avg_units_sold AS (SELECT
									item,
									location,
									SUM(units_sold)/IFNULL(NULLIF(%s, 0), 1) AS units_sold  #p_year_interval
								FROM
									item_loc_yr_units
							GROUP BY
									item,
									location),
				sales_data AS (SELECT
									av.item,
									av.location,
							COALESCE(CASE
										WHEN av.units_sold > ily.units_sold THEN av.units_sold
										ELSE ily.units_sold
									END,
			                av.units_sold) AS final_units_sold
								FROM avg_units_sold av
								LEFT JOIN item_loc_yr_units ily ON av.item = ily.item AND av.location = ily.location AND ily.year_difference = 1),
			   item_soh_data AS (SELECT
										ils.item,
			                            ils.location,
			                            SUM(GREATEST(GREATEST(CASE WHEN ilc.status IN ('A', 'C') THEN ils.item_soh ELSE 0 END,0)
			                                    - (
			                                        GREATEST(CASE WHEN ilc.status IN ('A', 'C') THEN ils.reserved_qty ELSE 0 END, 0)
			                                        +GREATEST(CASE WHEN ilc.status IN ('A', 'C') THEN ils.rtv_qty ELSE 0 END, 0)
			                                       -- +GREATEST(CASE WHEN ilc.status IN ('A', 'C') THEN ils.non_sellable_qty ELSE 0 END, 0)
			                                       -- +GREATEST(CASE WHEN ilc.status IN ('A', 'C') THEN ils.customer_resv ELSE 0 END, 0)
			                                    ),0)) AS item_soh
									FROM
			                            item_location ils
			                            JOIN item_location ilc ON ils.item = ilc.item AND ils.location = ilc.location
			                        WHERE
			                            (ils.item, ils.location) IN (SELECT item, location FROM sales_data)
			                        GROUP BY
			                            ils.item,
			                            ils.location)
			        SELECT
			            sd.item,
			            sd.location,
			            sd.final_units_sold as total_units_sold,
			            isd.item_soh,
			            CASE
			                WHEN sd.final_units_sold > isd.item_soh THEN sd.final_units_sold - isd.item_soh
			                ELSE 0
			            END AS sales_lost
			        FROM
			            sales_data sd
			            JOIN item_soh_data isd ON sd.item = isd.item AND sd.location = isd.location;"""
		print(Q_sel_qty%(I_year_interval,I_sdate,I_sdate,I_weeks,I_sdate,I_hier1,I_hier1,I_hier1,I_hier2,I_hier2,I_hier2,I_hier3,I_hier3,I_hier3,I_sdate,I_year_interval,))
		df_mysql=pd.read_sql(Q_sel_qty%(I_year_interval,I_sdate,I_sdate,I_weeks,I_sdate,I_hier1,I_hier1,I_hier1,I_hier2,I_hier2,I_hier2,I_hier3,I_hier3,I_hier3,I_sdate,I_year_interval,),conn)
		
		#df_mysql=pd.read_sql(Q_sel_qty,con=conn,params=(I_year_interval,I_sdate,I_sdate,I_weeks,I_sdate,I_hier1,I_hier1,I_hier1,I_hier2,I_hier2,I_hier2,I_hier3,I_hier3,I_hier3,I_sdate,I_year_interval,))
		print(df_mysql)
		return df_mysql, ''


	except Exception as Argument:
		err_return = ""
		if O_status==1:
			err_return = L_func+":"+str(O_status)+": Exception raised while selecting the records: "+ str(Argument)
			print("sales_diff_qty: Exception raised while selecting the records ",I_year_interval,I_sdate,I_weeks,I_hier1,I_hier2,I_hier3)
		else:
			err_return = L_func+":"+str(O_status)+": Exception raised: "+ str(Argument)
			print( "sales_diff_qty: Exception Occured", Argument)
			return [], err_return




#if __name__ == "__main__":
#	o_status=none
#	i_year_interval = 3
#	i_sdate = '2024-08-04'
#	i_weeks = 2
#	i_hier1 = 176
#	i_hier2 = 1176
#	i_hier3 = 10176
#	daily_view = sales_diff_qty(o_status,i_year_interval,i_sdate,i_weeks,i_hier1,i_hier2,i_hier3)  
#	print(daily_view);

