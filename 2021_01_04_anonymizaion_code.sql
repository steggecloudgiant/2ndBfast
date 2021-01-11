--ANONYMIZE CUSTOMER DATA

CREATE table "tcp-ds-anonymized".customer
WITH (format='parquet',external_location = 's3://tcp-ds-eu-west-1-1tb-anonymised/2/customer_parquet/')
AS SELECT       
         sha256(to_utf8(cast(c_customer_sk AS varchar))) AS c_customer_sk_anonym,
         sha256(to_utf8(cast(c_customer_id AS varchar))) AS c_customer_id_anonym,
         sha256(to_utf8(cast(c_first_name AS varchar))) AS c_first_name_anonym,
         sha256(to_utf8(cast(c_last_name AS varchar))) AS c_last_name_anonym,
         c_current_cdemo_sk,
         c_current_hdemo_sk,
         c_first_shipto_date_sk,
         c_first_sales_date_sk,
         c_salutation,
         c_preferred_cust_flag,
         c_current_addr_sk,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_last_review_date_sk
FROM customer;


--ANONYMIZE SALES DATA

CREATE table "tcp-ds-anonymized".store_sales
WITH (format='parquet',external_location = 's3://tcp-ds-eu-west-1-1tb-anonymised/1/store_sales/')
AS SELECT sha256(to_utf8(cast(ss_customer_sk AS varchar))) AS ss_customer_sk_anonym,
         ss_sold_date_sk,
         ss_sales_price,
         ss_sold_time_sk,
         ss_item_sk,
         ss_hdemo_sk,
         ss_addr_sk,
         ss_store_sk,
         ss_promo_sk,
         ss_ticket_number,
         ss_quantity,
         ss_wholesale_cost,
         ss_list_price,
         ss_ext_discount_amt,
         ss_external_sales_price,
         ss_ext_wholesale_cost,
         ss_ext_list_price,
         ss_ext_tax,
         ss_coupon_amt,
         ss_net_paid,
         ss_net_paid_inc_tax,
         ss_net_profit
FROM store_sales;
