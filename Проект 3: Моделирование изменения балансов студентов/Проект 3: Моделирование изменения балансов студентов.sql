with

    first_payments as 
    (
    select user_id, 
           date_trunc('day', min(transaction_datetime)) as first_payment_date
    from skyeng_db.payments
    where status_name = 'success'
    group by 1
    ),
    
    all_dates as -- Шаг 2
    (
        select distinct class_start_datetime::date as dt
        from skyeng_db.classes
        where class_start_datetime >= '01.01.2016 00:00'
              and class_start_datetime < '01.01.2017 00:00'
    ),
    
    all_dates_by_user as -- Шаг 3
    (
        select a.user_id
             , b.dt
        from first_payments a
        join all_dates b
        on b.dt >= a.first_payment_date
    ),
    
    payments_by_dates as -- Шаг 4
    (
        select user_id
             , date_trunc ('day', transaction_datetime) as payment_date
             , sum (classes) as transaction_balance_change
        from skyeng_db.payments
        where status_name = 'success'
        group by user_id, payment_date
    ),
    
    payments_by_dates_cumsum as -- Шаг 5
    (
        select a.user_id
             , a.dt
             , transaction_balance_change
             , sum (COALESCE(transaction_balance_change, 0)) over (partition by a.user_id order by dt rows between unbounded preceding and current row) as transaction_balance_change_cs
            from all_dates_by_user a
            left join payments_by_dates b
            on a.user_id = b.user_id
            and a.dt = b.payment_date
    ),
    
    classes_by_dates as -- Шаг 6
    (
        select user_id
             , date_trunc ('day', class_start_datetime) as class_date
             , count (id_class) * -1 as classes 
        from skyeng_db.classes
        where class_status in ('success', 'failed_by_student')
        and class_type != 'trial'
        group by 1, 2
    ),

    classes_by_dates_dates_cumsum as -- Шаг 7
    (
        select a.user_id
             , dt
             , classes
             , sum (COALESCE(classes, 0)) over (partition by a.user_id order by dt rows between unbounded preceding and current row) as classes_cs
            from all_dates_by_user a
            left join classes_by_dates b
            on a.user_id = b.user_id
            and a.dt = b.class_date
    ),

    balances as -- Шаг 8
    (
        select a.user_id
             , a.dt
             , transaction_balance_change
             , transaction_balance_change_cs
             , classes
             , classes_cs
             , classes_cs + transaction_balance_change_cs as balance
        from payments_by_dates_cumsum a
        join classes_by_dates_dates_cumsum b
        on a.user_id = b.user_id
        and a.dt = b.dt
    )


-- select * -- Задание 1
-- from balances
-- order by user_id, dt
-- limit 1000

select -- Шаг 9
      dt
     , sum (transaction_balance_change) as transaction_balance_change
     , sum (transaction_balance_change_cs) as transaction_balance_change_cs
     , sum (classes) as classes
     , sum (classes_cs) as classes_cs
     , sum (balance) as balance
from balances
group by dt
order by dt
