set mapred.reduce.tasks=80;
select i_item_desc, i_category, i_class, i_current_price, ss_ext_sales_price
from  store_sales join item on (store_sales.ss_item_sk = item.i_item_sk)
where ss_item_sk = i_item_sk and i_category = 'Jewelry'
limit 100;
exit;
