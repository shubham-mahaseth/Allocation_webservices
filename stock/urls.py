"""stock URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('',   views.home,   name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('',   Home.as_view(),   name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include,   path
    2. Add a URL to urlpatterns:  path('blog/',   include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from stock_ledger_models.views_err import err_trn, del_err_trn_data, err_trn_data_table

from stock_ledger_models.views_daily import (
    count_pndg_dly_rollup,
    daily_sku_table,
    daily_rollup_table,
    daily_rec_table,
    Daily_view,
)

from stock_ledger_models.views_stage import (
    count_stg_trn_data,
    stg_trn,
    retrieve_stg,
    retrieve_err_stg,
    stg_fin,
)

from stock_ledger_models.views_global import (
    cancel_transaction,
    system_conf,
    location_valid,
    currency_valid,
    item_location_valid,
    get_cost_item_location,
    cost_update_stg,
    lov_item_dtl,
    system_config_table,
    fetch_item_location,
    sub_cost,
    system_config_creation,
)

from stock_ledger_models.views_tran import (
    count_trn_data,
    trn_data_table,
    trn_data_history_table,
    trn_data_rev_table,
    trn_data_rev_1_table,
    trn_type_dtl_table,
    trn_type_dtl_list,
    trn_hist_data,
)

from stock_ledger_models.views import (
    sample,
    GL_ACCOUNT_table,
    GL_ACCOUNT_update,
    GL_ACCOUNT_INSERT,
    item_valid,
    currency_gl,
    primary_gl,
)

from stock_ledger_models.views_allocation import (
    Alloc_result_PO_table,
    Alloc_result_WH_table,
    Alloc_result_ASN_table,
    Alloc_result_TSF_table,
    Alloc_avail_qty_table,
    Alloc_avail_search_table_grid,
    Alloc_delete_Create_table,
    Alloc_Refresh_grid_Create_table,
    Alloc_switchTab,
    Alloc_update_SelInd_Create_table,
    calculation,
    err_report,
    AllocSumm_switchTab,
    AllocSumm_search,
    AllocSumm_CopyAlloc,
    approve_createScreen_table,
    approve_valid_createScreen_table,
    createScreen_grid_table,
    ASMY_Validation,
    reverse_createScreen_table,
    worksheet_createScreen_table,
    schdl_save,
    schdl_rtv,
    Alloc_split_button_create_table,
    Alloc_Commit_Data_table,
    massApprove,
    log_active_session,
    delete_active_session,
    update_actv_session,
    Report
)

from stock_ledger_models.views_header import (
    Alloc_Type_table,
    Alloc_Level_table,
    Context_type_table,
    Status_table,
    Promotion_table,
    Criteria_table,
    Alloc_No_table,
    Alloc_no_screen_table,
    Retrieve_Comment_Data,
    Insert_Comment_Data,
)

from stock_ledger_models.views_alloc_criteria import (
    HIER1_table,
    HIER3_table,
    HIER2_table,
    UDA_table,
    EXCLUDE_UDA_table,
    ITEM_PARENT_table,
    DIFF_table,
    SKU_table,
    item_list_head_table,
    Warehouse_table,
    SUPPLIER_table,
    SUPPLIER_SITE_table,
    PACK_NO_table,
    VPN_table,
    PO_table,
    ASN_table,
    TSF_table,
    del_MapItems,
    map_AllocItems,
    Like_item_Insert,
    Alloc_no_ASY,
    Alloc_ASY,
    PO_TYPE_table,
    getNoSkus,
    Multi_PO_Create_Table,
)

from stock_ledger_models.views_Qlimits import (
    Alloc_qty_limits_retrive_table,
    alloc_qty_Limits_Insert_table,
    qty_Limits_Rtv,
    update_Alloc_quantity_limits_grid_table,
)

from stock_ledger_models.views_location import (
    Alloc_INS_DATA_table,
    Alloc_DEL_LOCS_table,
    alloc_rtv_rl_data_table,
    Alloc_pop_store_table,
    store_table,
    store_list_table,
    store_traits_table,
    rule_type_code_detail_table,
    hierarchy_code_detail_table,
    need_code_detail_table,
    allocate_to_code_detail_table,
    clearance_code_detail_table,
    status_code_detail_table,
    get_LocationGrid,
    alloc_rule_Data,
    Fetch_Alloc_change_weights_table,
    RL_Data_check_table,
    RL_ruleTemplateData,
    RL_locTemplateData,
    rule_template_name_data,
    loc_template_name_data,
    Fetch_RuleTemplateData,
    Fetch_locTemplateData,
)

from stock_ledger_models.views_WhatIF import (
    POType_WhatIF_table,
    Supplier_WhatIF_table,
    Origin_country_WhatIF_table,
    Retrieve_WhatIF_table,
    Submit_WhatIF_table,
    populatePoPreview,
    createPO,
    update_POQty,
)

from stock_ledger_models.views_AllocDetails import (
    allocDetails_commit,
    Alloc_dtl_validation,
    spread_Alloc,
    Update_AllocQty,
    Size_Profile,
    fetch_net_need,
    copyD_AD,
    # ADP
    Alloc_DPack_data,
    AD_validation,
    pck_Store_Data,
    pck_Comp_Data,
    restore_ADPk,
    update_ADPk,
)
from stock_ledger_models.views_sizeDetails import (
    sizeDetails_Commit,
    size_details_table,
    size_details_Header_table,
    size_details_Update_table,
)
from stock_ledger_models.views_dashboard import (
    Alloc_Dashboard_UserAlloc_table,
    Alloc_Dashboard_Release_table,
    Alloc_Dashboard_AllocCount_table,
    Dashboard_Allocations
)
from stock_ledger_models.views_AllocationBatches import (
    schedule_Batch,
    Update_Batch_Date,
)
from stock_ledger_models.views_SeedData import (
    insert_into_hier1,    
    insert_into_hier2,
    insert_into_hier3,
    insert_into_item_dtl
)

urlpatterns = [
    path("admin/", admin.site.urls),
    # ********************* #
    #     STOCK LEDGER      #
    # ********************* #

    path(
        "err_trn_data/", err_trn
    ),  # Fetching all the column values from ERR_TRN_DATA table.
    path(
        "delete_err_trn/", del_err_trn_data
    ),  # Deleting the records from ERR_TRN_DATA table and updating in STG_TRN_DATA table.
    path(
        "err_trn_data_tab/", err_trn_data_table
    ),  # Fetching the data from ERR_TRN_DATA table based on the input parameters.
    path(
        "count_pending/", count_pndg_dly_rollup
    ),  # count of different indicators in PNDG_DLY_ROLLUP table.
    path(
        "daily_sku/", daily_sku_table
    ),  # Fetching the data from DAILY SKU based on the input parameters.
    path(
        "daily_rollup/", daily_rollup_table
    ),  # Fetching the data from DAILY ROLLUP based on the input parameters.
    path(
        "count_stg_trn/", count_stg_trn_data
    ),  # count of different indicators in STG_TRN_DATA table.
    path(
        "stg_trn_data/", stg_trn
    ),  # Inserting random TRAN_SEQ_NO in the STG_TRN_DATA table.
    path(
        "retrieve_stg_trn_data/", retrieve_stg
    ),  # Retrieve filtered data STG_TRN_DATA table using input parameters user and date.
    path(
        "count_trn/", count_trn_data
    ),  # count of different indicators in TRN_DATA table.
    path(
        "trn_data/", trn_data_table
    ),  # Fetching the data from TRN_DATA based on the input parameters.
    path(
        "trn_data_history/", trn_data_history_table
    ),  # Fetching the data from TRN DATA HISTORY based on the input parameters.
    path(
        "trn_data_rev/", trn_data_rev_table
    ),  # Transaction reversal(Fetch the record from TRN_DATA_HISTORY table,   insert the original data in TRN_DATA_REV,   insert the updated record values in STG_TRN_DATA table & insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)
    path(
        "cancel/", cancel_transaction
    ),  # Transaction reversal(Fetch the record from TRN_DATA_HISTORY table,   insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)
    path(
        "trn_data_rev_1/", trn_data_rev_1_table
    ),  # Transaction reversal(Fetch the record from TRN_DATA_HISTORY table,   insert the original data in TRN_DATA_REV,   insert the updated record values in STG_TRN_DATA table with new TRAN_SEQ_NO & call the another webservice(cancel_transaction))
    path(
        "location_validation/", location_valid
    ),  # location validation from LOCATION table.
    path(
        "currency_validation/", currency_valid
    ),  # currency validation from CURRENCY table.
    path(
        "item_location_validation/", item_location_valid
    ),  # item and location validation from ITEM_LOCATION table.
    path(
        "get_cost_item_location_valid/", get_cost_item_location
    ),  # Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location.
    path(
        "cost_update_stg_trn/", cost_update_stg
    ),  # Update and Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location new_cost,   also update STG_TRN_DATA.
    path(
        "retrieve_stg_trn_data/", retrieve_stg
    ),  # Retrieve filtered data STG_TRN_DATA table using input parameters user and date.
    path(
        "sys_conf/", system_conf
    ),  # Fetching the data from SYSTEM CONFIG table based on the TRN_TYPE and updated the record with new values.
    path(
        "lov_item_dtl/", lov_item_dtl
    ),  # "ITEM",  "ITEM_DESC",  "CLASS",  "DEPT",  "SUBCLASS" validation from lov_item_dtl .
    path(
        "system_config_tab/", system_config_table
    ),  # Fetching the data from SYSTEM_CONFIG based on the input parameters:
    path(
        "gl_account_tab/", GL_ACCOUNT_table
    ),  # Fetching the data from GL_ACCOUNT based on the input parameters:
    path("item_loc_data/", fetch_item_location),  # Fetch data from ITEM_LOCATION tables
    path(
        "daily_rec/", daily_rec_table
    ),  # Fetching the data from DAILY SKU based on the input parameters.
    path(
        "retrieve_err_stg_data/", retrieve_err_stg
    ),  # Retrieve filtered data from ERR_TRN_DATA and STG_TRN_DATA table using input parameters user and date.
    path(
        "GL_ACCOUNT_update/", GL_ACCOUNT_update
    ),  # UPDATING - GL_ACCOUNT based on the input
    path(
        "GL_ACCOUNT_create/", GL_ACCOUNT_INSERT
    ),  # Insert the input data to GL account.
    path("item_validation/", item_valid),
    path("currency_gl/", currency_gl),
    path("Retrieve_stg_fin/", stg_fin),
    path("trn_type_dtl/", trn_type_dtl_table),
    path("trn_type_dtl_list/", trn_type_dtl_list),
    path("Daily_view/", Daily_view),
    path("subledger_cost/", sub_cost),
    path("system_config_creation/", system_config_creation),
    path("primary_gl/", primary_gl),
    path("test/", sample),
    path("inv_hist/",trn_hist_data),


    # ******************************************** #
    #     ALLOCATION DETAILS (PACK/NON-PACK)      #
    # ******************************************** #
    path("adSave/",allocDetails_commit),
    path("Alloc_dtl_tab/", Alloc_dtl_validation),
    # path('Alloc_dtl_data/',Alloc_details_data),
    path("spreadAlloc/", spread_Alloc),
    path("UAllocQty/", Update_AllocQty),
    path("copyDownAD/", copyD_AD),
    path("fetchNN/", fetch_net_need),
    path("sizePro/", Size_Profile),
    path("rtvADPack/", Alloc_DPack_data),
    path("AD_valid/", AD_validation),
    path("ADPgrid2/", pck_Store_Data),
    path("ADPgrid1/", pck_Comp_Data),
    path("ADPRestore/", restore_ADPk),
    path("ADPUpdate/", update_ADPk),
    # ********************************#
    #         SIZE DETAILS            #
    # ********************************#
    path("sdSave/",sizeDetails_Commit),
    path("size_details_tab/", size_details_table),
    path("size_details_Header_tab/", size_details_Header_table),
    path("size_details_Update_tab/", size_details_Update_table),
    # ********************************#
    #        ALLOCATION               #
    # ********************************#
    path("Alloc_result_tab/", Alloc_result_PO_table),
    path("Alloc_result_WH_tab/", Alloc_result_WH_table),
    path("Alloc_result_ASN_tab/", Alloc_result_ASN_table),
    path("Alloc_result_TSF_tab/", Alloc_result_TSF_table),
    path("Alloc_avail_search_tab/", Alloc_avail_search_table_grid),
    path("Alloc_update_SelInd_Create_tab/", Alloc_update_SelInd_Create_table),
    path("Alloc_delete_Create_tab/", Alloc_delete_Create_table),
    path("Alloc_Refresh_grid_Create_tab/", Alloc_Refresh_grid_Create_table),
    path("calculation_tab/", calculation),
    path("err_report_tab/", err_report),
    path("approve_createScreen_tab/", approve_createScreen_table),
    path("approve_valid_createScreen_tab/", approve_valid_createScreen_table),
    path("createScreen_grid_tab/", createScreen_grid_table),
    path("switch_ASY/", AllocSumm_switchTab),
    path("search_asmy/", AllocSumm_search),
    path("copy_alloc/", AllocSumm_CopyAlloc),
    path("valid_asy/", ASMY_Validation),
    path("reverse_createScreen_tab/", reverse_createScreen_table),
    path("worksheet_createScreen_tab/", worksheet_createScreen_table),
    path("Alloc_split_button_create_tab/", Alloc_split_button_create_table),
    path("schdlsv/", schdl_save),
    path("schdlData/", schdl_rtv),
    path("Alloc_Commit_Data_tab/", Alloc_Commit_Data_table),
    path("massAprv/",massApprove),
    path("allocLog/",log_active_session),
    path("delSession/",delete_active_session),
    path("resetSession/",update_actv_session),
    path("genReport/",Report),

    # ********************************#
    #       ALLOCATION CRITERIA      #
    # ********************************#

    path("HIER_tab/", HIER1_table),
    path("HIER2_tab/", HIER2_table),
    path("HIER3_tab/", HIER3_table),
    path("UDA_tab/", UDA_table),
    path("EXCLUDE_UDA_tab/", EXCLUDE_UDA_table),
    path("item_list_head_tab/", item_list_head_table),
    path("ITEM_PARENT_tab/", ITEM_PARENT_table),
    path("DIFF_tab/", DIFF_table),
    path("SKU_tab/", SKU_table),
    path("Warehouse_tab/", Warehouse_table),
    path("SUPPLIER_tab/", SUPPLIER_table),
    path("SUPPLIER_SITE_tab/", SUPPLIER_SITE_table),
    path("PACK_NO_tab/", PACK_NO_table),
    path("VPN_tab/", VPN_table),
    path("PO_tab/", PO_table),
    path("ASN_tab/", ASN_table),
    path("TSF_tab/", TSF_table),
    path("alloc_dtls_asy/", Alloc_no_ASY),
    path("alloc_asy/", Alloc_ASY),
    path("PO_TYPE_tab/", PO_TYPE_table),
    path("Multi_PO_Create_Tab/", Multi_PO_Create_Table),

    # ********************************#
    #       ALLOCATION HEADER         #
    # ********************************#

    path("Alloc_Type_tab/", Alloc_Type_table),
    path("Alloc_Level_tab/", Alloc_Level_table),
    path("Context_type_tab/", Context_type_table),
    path("Status_tab/", Status_table),
    path("Promotion_tab/", Promotion_table),
    path("Criteria_tab/", Criteria_table),
    path("Alloc_No_tab/", Alloc_No_table),
    path("Alloc_no_screen_tab/", Alloc_no_screen_table),
    path("Alloc_avail_qty_tab/", Alloc_avail_qty_table),
    path("switchTab/", Alloc_switchTab),
    path("rtvcomments/",Retrieve_Comment_Data),
    path("inscomment/",Insert_Comment_Data),

    # ********************************#
    #       QUANTITY LIMITS           #
    # ********************************#
    path("Alloc_qty_limits_retrive_tab/", Alloc_qty_limits_retrive_table),
    path("alloc_qty_Limits_Insert_tab/", alloc_qty_Limits_Insert_table),
    path("Retrieve_qtyLmt/", qty_Limits_Rtv),
    path(
        "update_Alloc_quantity_limits_grid_tab/",
        update_Alloc_quantity_limits_grid_table,
    ),

    # ********************************#
    #       LIKE ITEM MAPPING         #
    # ********************************#

    # path('allocated_items/',  get_Allocated_item_dtls),
    path("Insert_LikeItem/", Like_item_Insert),
    path("delMappedItms/", del_MapItems),
    path("mapItms/", map_AllocItems),
    path("noSkus/", getNoSkus),

    # ********************************#
    #            RULES                #
    # ********************************#

    path("alloc_rule_Data_tab/", alloc_rule_Data),
    path("Fetch_Alloc_change_weights_tab/", Fetch_Alloc_change_weights_table),

    # ********************************#
    #            LOCATION             #
    # ********************************#

    path("store_tab/", store_table),
    path("store_list_table/", store_list_table),
    path("store_traits_tab/", store_traits_table),
    path("rule_type_code_detail_tab/", rule_type_code_detail_table),
    path("hierarchy_code_detail_tab/", hierarchy_code_detail_table),
    path("need_code_detail_tab/", need_code_detail_table),
    path("allocate_to_code_detail_tab/", allocate_to_code_detail_table),
    path("clearance_code_detail_tab/", clearance_code_detail_table),
    path("status_code_detail_tab/", status_code_detail_table),
    path("locationGrid/", get_LocationGrid),
    path("alloc_rtv_rl_data_tab/", alloc_rtv_rl_data_table),
    path("Alloc_DEL_LOCS_tab/", Alloc_DEL_LOCS_table),
    path("Alloc_pop_store_tab/", Alloc_pop_store_table),
    path("Alloc_INS_DATA_tab/", Alloc_INS_DATA_table),
    path("RL_Data_check_tab/", RL_Data_check_table),
    path("ruleRLTemplateData/",RL_ruleTemplateData),
    path("locRLTemplateData/",RL_locTemplateData),
    path("rule_template_data/",rule_template_name_data),
    path("loc_template_data/",loc_template_name_data),
    path("Fetch_RL_RuleTemplateData/",Fetch_RuleTemplateData),
    path("Ftech_RL_locTemplateData/",Fetch_locTemplateData),


    # ********************************#
    #            WHATIF               #
    # ********************************#
    path("POType_WhatIF_tab/", POType_WhatIF_table),
    path("Supplier_WhatIF_tab/", Supplier_WhatIF_table),
    path("Origin_country_WhatIF_tab/", Origin_country_WhatIF_table),
    path("Retrieve_WhatIF_tab/", Retrieve_WhatIF_table),
    path("Submit_WhatIF_tab/", Submit_WhatIF_table),
    path("rtvpopreview/", populatePoPreview),
    path("createpo/", createPO),
    path('updatePO/',update_POQty),

  
    # ***********************************#
    #            DASHBOARD               #
    # ***********************************#
    path('Alloc_Dashboard_UserAlloc_tab/',Alloc_Dashboard_UserAlloc_table),
    path('Alloc_Dashboard_Release_tab/',Alloc_Dashboard_Release_table),
    path('Alloc_Dashboard_AllocCount_tab/',Alloc_Dashboard_AllocCount_table),
    path('DAlloc/',Dashboard_Allocations),
    

    # ********************************************#
    #            ALLOCATION BATCHES               #
    # ********************************************#
    path('alcscdlbth/',schedule_Batch),
    path('updbthdate/',Update_Batch_Date),
    
    
    # ********************************************#
    #            SEED DATA INSERT                 #
    # ********************************************#
    path("insHier1/", insert_into_hier1),      
    path("insHier2/",insert_into_hier2),
    path("insHier3/",insert_into_hier3),
    path("insItmDtl/",insert_into_item_dtl),
]                         
