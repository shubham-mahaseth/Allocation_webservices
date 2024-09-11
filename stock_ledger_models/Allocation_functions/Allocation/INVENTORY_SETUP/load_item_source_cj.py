from asyncio.windows_events import NULL
from contextlib import nullcontext
from logging import NullHandler
import mysql.connector
from pickle import NONE
from types import NoneType
from ..GLOBAL_FILES.get_connection import get_mysql_conn
from .update_alloc_ext import update_alloc
from .inventory_setup import setup_item_location,setup_location
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd



#--------------------------------------------------------------
# Function to load_item_source 
#--------------------------------------------------------------
def load_item(conn,L_alloc_no,O_status):
    try:
        O_status = list()
        O_status.append(0)
        O_status[0]=0

        Q_sel_record="select * from alloc_rule where alloc_no=%s;"
        Q_alloc_level="""select alloc_level from alloc_head where alloc_no=%s;"""
        Q_sel_allitemloc="""select 1 from alloc_calc_allitemloc_temp where alloc_no=%s;"""

        Q_del_alloc_item_src_tmp="""DELETE FROM alloc_like_item_source WHERE item_source_id IN
                                     (select item_source_id
                                     FROM alloc_item_source_dtl it
                                     WHERE it.alloc_no=%s
                                     AND NOT EXISTS
                                     (SELECT 1
                                     FROM alloc_itm_search_dtl tmp
                                     WHERE   tmp.alloc_no = it.alloc_no
                                     AND tmp.item = it.item_id                
                                     AND ifnull(tmp.diff_id,'$') = ifnull(it.diff1_id,'$') 
                                     AND tmp.loc = it.wh_id
                                     AND ifnull(tmp.ref_1, '$') = ifnull(it.order_no, '$')
                                     AND tmp.sel_ind = 'Y'));"""

        Q_del_item_source="""with it as 
                            (select it.item_source_id,it.alloc_no from alloc_item_source_dtl it where alloc_no=%s                        
                            AND NOT EXISTS
                            (SELECT 1
                            FROM ALLOC_ITM_SEARCH_DTL tmp
                            WHERE   tmp.alloc_no = it.alloc_no
                            AND tmp.item = it.item_id                  
                            AND ifnull(tmp.diff_id,'$') = ifnull(it.diff1_id,'$')  
                            AND tmp.loc = it.wh_id
                            AND ifnull(tmp.ref_1, '$') = ifnull(it.order_no, '$')
                            AND tmp.sel_ind = 'Y'))
                            delete from alloc_item_source_dtl ais where ais.item_source_id in (select it.item_source_id from it where it.alloc_no=ais.alloc_no ) ;"""
        Q_merge="""with src as
                    (SELECT tmp.alloc_no,
                           tmp.item,
                           tmp.diff_id,
                           tmp.loc,
                           tmp.ref_1,
                           max(tmp.holdback_qty) holdback_qty,
                           /*Changes for O20p-74 start*/
                           max(case when tmp.holdback_type='U' then 'N' else tmp.holdback_type END) as holdback_type, 
                           /*Changes for O20p-74 end*/
                           max(tmp.som_qty) som_qty,
                           max(tmp.avail_qty) available_qty
                           #max(tmp.calc_multiple) calc_multiple
                      FROM alloc_itm_search_dtl tmp
                     WHERE alloc_no = %s 
                       AND tmp.sel_ind = 'Y'
                     GROUP BY tmp.alloc_no,
                              tmp.item,
                              tmp.diff_id,
                              tmp.loc,
                              tmp.ref_1)
                              
			  update alloc_item_source_dtl tgt,src SET
                tgt.hold_back_value = src.holdback_qty,
                tgt.hold_back_pct_flag = src.holdback_type,
                tgt.som_qty = src.som_qty,
                tgt.avail_qty = src.available_qty
                #tgt.calc_multiple = src.calc_multiple 
                     WHERE src.alloc_no = tgt.alloc_no
                    AND src.item = tgt.item_id
                    AND ifnull(src.diff_id,'$') = ifnull(tgt.diff1_id,'$')  
                    AND src.loc = tgt.wh_id
                    AND ifnull (src.ref_1, '$') = ifnull (tgt.order_no, '$')
					 and (   (ifnull (tgt.hold_back_value, 99999) <>
                                ifnull (src.holdback_qty, 99999))
                            OR (ifnull (tgt.hold_back_pct_flag, '$') <>
                                ifnull (src.holdback_type, '$'))
                            OR (ifnull (tgt.avail_qty, -99999999999) <>
                                ifnull (src.available_qty, -99999999999))
                            #OR (ifnull (tgt.calc_multiple, '$') <>
                              #  ifnull (src.calc_multiple, '$'))
                            OR (ifnull (tgt.som_qty, 999999) <>
                                ifnull (src.som_qty, 999999)));"""
        
        Q_ins_item_source="""insert into alloc_item_source_dtl(ALLOC_NO,ITEM_ID,DEFAULT_LEVEL,PACK_IND,HOLD_BACK_PCT_FLAG,HOLD_BACK_VALUE,SOM_QTY,AVAIL_QTY,RELEASE_DATE,SOURCE_TYPE,ORDER_NO,
                            WH_ID,DIFF1_ID,DIFF1_DESC,DIFF2_ID,INNER_SIZE,CASE_SIZE,PALLET,CALC_MULTIPLE,ON_HAND_QTY,FUTURE_ON_HAND_QTY,MIN_AVAIL_QTY,THRESHOLD_PERCENT)
				select ALLOC_NO,ITEM_ID,DEFAULT_LEVEL,PACK_IND,HOLD_BACK_PCT_FLAG,HOLD_BACK_VALUE,SOM_QTY,AVAIL_QTY,RELEASE_DATE,SOURCE_TYPE,ORDER_NO,
                            WH_ID,DIFF1_ID,DIFF1_DESC,DIFF2_ID,INNER_SIZE,CASE_SIZE,PALLET,CALC_MULTIPLE,ON_HAND_QTY,FUTURE_ON_HAND_QTY,MIN_AVAIL_QTY,THRESHOLD_PERCENT from 
                            (select
                            tmp.alloc_no as alloc_no,
                            tmp.item as item_id,
                            1 as DEFAULT_LEVEL,
                            (case when ah.alloc_level='T'
                            then 'N'ELSE tmp.pack_ind END) pack_ind,
                            max(case when tmp.holdback_type='Y' then 'Y' 
                                when tmp.holdback_type='U' then 'N' else tmp.holdback_type END) as hold_back_pct_flag,
                            max(tmp.holdback_qty) as hold_back_value,
                            max(tmp.som_qty) as som_qty,
                            max(avail_qty) as avail_qty,
                            current_date as release_date,
                            (CASE
                                WHEN tmp.ALLOC_CRITERIA  IS NULL THEN 3
                                WHEN tmp.ALLOC_CRITERIA  = 'P' THEN 1
                                WHEN tmp.ALLOC_CRITERIA  = 'A' THEN 2 
                                WHEN tmp.ALLOC_CRITERIA  = 'T' THEN 6 
                                WHEN tmp.ALLOC_CRITERIA  = 'F' THEN 5 
                                END ) source_type,
                            tmp.ref_1 as order_no,
                            tmp.loc as wh_id,
                            tmp.diff_id as diff1_id,
                            null as diff1_desc,
                            null as diff2_id,
                            null as inner_size,
                            null as case_size,
                            1 PALLET,
                            tmp.som_qty as calc_multiple,
                            0 ON_HAND_QTY,
                            0 FUTURE_ON_HAND_QTY,
                            0 MIN_AVAIL_QTY,
                            0 THRESHOLD_PERCENT
                            from alloc_itm_search_dtl tmp,
                            alloc_head ah
                            where tmp.alloc_no=%s
                            group by tmp.item,tmp.loc) tmp
                            where not exists
                            (SELECT 1
                          FROM alloc_item_source_dtl it
                         WHERE     it.alloc_no = tmp.alloc_no
                               AND it.item_id = tmp.item_id
                               AND ifnull(it.diff1_id,'$') = ifnull(tmp.diff1_id,'$')                            
                               AND it.wh_id = tmp.wh_id
                               AND ifnull(it.order_no, '$') = ifnull(tmp.order_no, '$')); """

        Q_upd_pack_ind="""WITH OK AS (SELECT DISTINCT tmp.item item,
							tmp.alloc_no,
							(CASE WHEN tmp.pack_ind='Y'  AND id.sellable_ind='Y' THEN 'SP' 
								 WHEN tmp.pack_ind='Y' and id.sellable_ind='N' THEN 'NS' 
								 WHEN tmp.pack_ind='N' THEN 'N'
								END)  pack_ind
                          FROM alloc_itm_search_dtl tmp join
                               item_dtl id 
                        WHERE tmp.alloc_no=%s
							    AND tmp.item=id.item)
							UPDATE alloc_item_source_dtl T1 SET T1.PACK_IND=(SELECT PACK_IND FROM OK WHERE OK.ALLOC_NO=T1.ALLOC_NO AND OK.ITEM=T1.ITEM_ID);"""
        
        Q_del_qty="""WITH table1 AS 
                    (SELECT alloc_no,
                            item_id,
                            location_id
                       FROM alloc_quantity_limits
                      WHERE alloc_no = %s),
                    table2 AS 
                    (SELECT tmp.alloc_no,
                            tmp.tran_item   item_id,
                            tmp.to_loc    location_id
                       FROM alloc_calc_allitemloc_temp tmp
                      WHERE alloc_no = %s
                        AND coalesce(item_type,'$') <> 'NSFSP')
                    DELETE FROM alloc_quantity_limits gtt
                     WHERE alloc_no = %s
                       AND (alloc_no, item_id, location_id) IN (SELECT table1.alloc_no, 
                                                                           table1.item_id, 
                                                                           table1.location_id FROM table1
                                                                  LEFT JOIN table2 
                                                                    ON table1.alloc_no = table2.alloc_no);"""
        
        Q_del_qty_nsfsp="""DELETE  FROM alloc_quantity_limits tmp
                      WHERE tmp.alloc_no = %s
                        AND EXISTS (SELECT 1
                                      FROM ALLOC_CALC_ALLITEMLOC_TEMP aacat
                                     WHERE aacat.alloc_no = tmp.alloc_no
                                       AND aacat.source_item = tmp.item_id
                                       AND aacat.item_type = 'NSFSP');"""

        mycursor = conn.cursor()
        O_status[0]=10
        L_item_found=0
        mycursor.execute(Q_alloc_level,(L_alloc_no,))
        c_alloc_level = mycursor.fetchall()
        df_alloc_lvl=pd.DataFrame(c_alloc_level)          
        df_alloc_lvl.columns=mycursor.column_names
        df_alloc_lvl=df_alloc_lvl["alloc_level"][0]

        O_status[0]=20
        mycursor.execute(Q_del_alloc_item_src_tmp,(L_alloc_no,))

        mycursor.execute(Q_del_item_source,(L_alloc_no,))

        O_status[0]=30
        mycursor.execute(Q_merge,(L_alloc_no,))

        O_status[0]=40
        if (mycursor.rowcount)==0:
            mycursor.execute(Q_ins_item_source,(L_alloc_no,))
            myresult = mycursor.fetchall()
        if mycursor.rowcount>0:
            L_item_found=1

        O_status[0]=50
        if df_alloc_lvl=='T':
            mycursor.execute(Q_upd_pack_ind,(L_alloc_no,))

        O_status[0]=60
        if L_item_found==1:
            L_fun_upd_alloc =update_alloc(conn,L_alloc_no,None,None,'Y',O_status)
            if L_fun_upd_alloc==False:
                print("inside function update_alloc")
                conn.rollback()
                return False
        
        O_status[0]=70
        if L_item_found==1:
            L_fun_calc_source=setup_item_location(conn,L_alloc_no,O_status)
            if L_fun_calc_source==False:
                print("setup_item_location function")
                conn.rollback()
                return False          
        
            O_status[0]=80
            mycursor.execute(Q_sel_allitemloc,(L_alloc_no,))
            myresult = mycursor.fetchall()
            if mycursor.rowcount>=1:
                    mycursor.execute(Q_sel_record,(L_alloc_no,))
                    L_rule = mycursor.fetchall()
                    if mycursor.rowcount>=1:
                        L_fun_setup_loc=setup_location(conn,L_alloc_no,O_status)
                        if L_fun_setup_loc==False:
                            print("aaaa")
                            conn.rollback()
                            return False

            O_status[0]=90
            mycursor.execute(Q_del_qty,(L_alloc_no,L_alloc_no,L_alloc_no,))
            myresult=mycursor.fetchall()
    
            mycursor.execute(Q_del_qty_nsfsp,(L_alloc_no,))
            myresult=mycursor.fetchall()
        conn.commit()
        return True
      
    except Exception as argument:
        if O_status[0]==10:
            print("load_item: Exception occured in selecting the alloc level: ",load_item,argument)
        elif O_status[0]==20:
            print("load_item: Exception occured in deleting the data: ",load_item,argument)
        elif O_status[0]==30:
            print("load_item:Exception occured while merging the data in alloc_item_source_dtl : ",load_item,argument)
        elif O_status[0]==40:
            print("load_item:Exception occured while inserting the data in alloc_item_source_dtl ",load_item,argument)
        elif O_status[0]==50:
            print("load_item:Exception occured while updating the pack indicator in alloc_item_source_dtl: ",load_item,argument)
        elif O_status[0]==60:
            print("load_item:Exception occured while calling the update alloc ext function: ",load_item,argument)
        elif O_status[0]==70:
            print("load_item:Exception occured while calling the setup_item_location function: ",load_item,argument)
        elif O_status[0]==80:
            print("load_item:Exception occured while calling the setup_location function: ",load_item,argument)
        elif O_status[0]==90:
            print("load_item:Exception occured while deleting the records from quqntity limits: ",load_item,argument)   
        conn.rollback()
        return False


