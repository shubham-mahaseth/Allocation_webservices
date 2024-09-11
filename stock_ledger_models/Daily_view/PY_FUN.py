#from stock_ledger_models.get_connection import get_mysql_conn
from mysql_conn import get_mysql_conn
import pandas as pd

def P360_SKU_AVAIL_QTY(L_item_parent,L_diff):
        try:
            I_get_mysql_conn = list()
            I_get_mysql_conn.append(0)
            print(I_get_mysql_conn)
            with get_mysql_conn (I_get_mysql_conn) as conn:
                a_query = """SELECT gtt.item,
                                    gtt.diff1,
                                    gtt.diff2,
                                    gtt.diff3,
                                    gtt.diff4,
                                    gtt.location location,
                        SUM(GREATEST ( (  GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.item_soh,0) ELSE 0 END), 0))
                                     - (  GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.reserved_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.rtv_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.non_sellable_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.cust_resv_qty,0) ELSE 0 END), 0))
                                     - (SELECT IFNULL (SUM(GREATEST (COALESCE(d.distro_qty,0),0)),0)
                                          FROM alloc_head h,
										       po_item_loc ol,
											   po_dtl oh,
                                               alloc_dtl d,
                                               alloc_itm_search_dtl ad,
                                               item_location ilc1
                                         WHERE ad.item = gtt.item
                                           AND d.to_loc = ils.location
                                           AND ilc1.item = gtt.item
                                           AND ilc1.location = ils.location
                                           AND ilc1.status in ('A','C')
                                           AND h.status IN ('A', 'R')
										   AND h.po_no IS NOT NULL
										   AND ol.po_no = h.po_no
										   AND ol.item = ad.item
										   AND ol.location = d.to_loc
										   AND ol.received_qty > 0
										   AND oh.po_no = ol.po_no
										   AND oh.status IN ('A', 'C')
                                           AND ad.alloc_no = h.alloc_no
                                           AND d.alloc_no = h.alloc_no
                                           AND d.alloc_qty > 0
                                           AND d.distro_qty > 0),0))  available_qty,
                        SUM(GREATEST ( (     GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.item_soh,0) ELSE 0 END), 0))
                                        - (  GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.reserved_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.rtv_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.non_sellable_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.cust_resv_qty,0) ELSE 0 END), 0))
                                        - (SELECT IFNULL (SUM(GREATEST (COALESCE(d.distro_qty,0),0)),0)
                                             FROM alloc_head h,
                                                  po_item_loc      ol,
                                                  po_dtl     oh,
                                                  alloc_dtl d,
				                        		  alloc_itm_search_dtl ad,
				                        		  item_location ilc1
                                            WHERE ad.item = gtt.item
                                              AND d.to_loc = gtt.location
                                              AND ilc1.item = gtt.item
                                              AND ilc1.location = ils.location
                                              AND ilc1.status in ('I')
                                              AND h.status IN ('A', 'R')
                                              AND h.po_no IS NOT NULL
                                              AND ol.po_no = h.po_no
                                              AND ol.item = ad.item
                                              AND ol.location = d.to_loc
                                              AND ol.received_qty > 0
                                              AND oh.po_no = ol.po_no
                                              AND oh.status IN ('A', 'C')
				                        	  AND ad.alloc_no = h.alloc_no
                                              AND d.alloc_no = h.alloc_no
                                              AND d.alloc_qty > 0
                                              AND d.distro_qty > 0), 0))  inactive_qty	                   
                                        
                                FROM (SELECT id.item,
                                             id.diff1,
                                             id.diff2,
                                             id.diff3,
                                             id.diff4,
                                             il.location
                                       FROM ITEM_DTL id, ITEM_LOCATION il
                                      WHERE id.item_parent = '{STR1}'
                                        AND id.status ='A'
                                        AND id.item = il.item
                                        AND (id.diff1 = '{STR2}'
                                          OR id.diff2 = '{STR2}'
                                          OR id.diff3 = '{STR2}'
                                          OR id.diff4 = '{STR2}')) gtt,
                                            ITEM_LOCATION          ils
                               WHERE ils.item = gtt.item
                                 AND ils.status IN ('A', 'C', 'I')
                               GROUP BY gtt.item,
                                        gtt.diff1,
                                        gtt.diff2,
                                        gtt.diff3,
                                        gtt.diff4,
                                        gtt.location"""
            df_mysql=pd.read_sql(a_query.format(STR1 = L_item_parent, STR2 = L_diff),conn)
            return df_mysql
        except Exception as error:
            return 0

if __name__ == "__main__":
    IP = '105307295'
    DIFF = 'L105'
    x = P360_SKU_AVAIL_QTY(IP, DIFF)    
    print(x)