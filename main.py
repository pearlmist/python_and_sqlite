import pandas
import sqlite3

import pandas as pd

# создаем базу данных и устанавливаем соединение с ней
con = sqlite3.connect("booking.sqlite")
# открываем файл с дампом базой данных
f_damp = open('booking.db', 'r', encoding='utf-8-sig')
# читаем данные из файла
damp = f_damp.read()
# закрываем файл с дампом
f_damp.close()
# запускаем запросы
con.executescript(damp)
# сохраняем информацию в базе данных
con.commit()
# создаем курсор
cursor = con.cursor()
# выбираем и выводим записи из таблиц author, reader
cursor.execute("SELECT * FROM room")
cursor2 = con.cursor()
cur = cursor.fetchone()
# rooms = []
pd.set_option('display.max_colwidth', None)
# вариант 30
# задание 1
temp = pd.read_sql('''
        SELECT 
            room_name AS Номер,
            type_room_name AS Тип_номера,
            price AS Цена
        FROM 
            room 
            NATURAL JOIN type_room
        WHERE 
            substr(Room_name, 3, 2) IN ('09', '10')
        ORDER BY 
            price ASC, 
            room_name ASC
            ''', con)
print(temp)

# задание 2
temp2 = pd.read_sql('''
        SELECT
            service_name as Услуга,
            CASE 
                WHEN SUM(price) IS NULL
                    THEN "-"
                ELSE COUNT(service_id)
            END AS Количество,
            CASE 
                WHEN ROUND(AVG(price), 2) IS NULL
                    THEN "-"
                ELSE ROUND(AVG(price), 2)
            END AS Средняя_цена,
            CASE 
                WHEN SUM(price) IS NULL
                    THEN "-"
                ELSE SUM(price)
            END AS Сумма
        FROM
            service
            LEFT JOIN service_booking USING (service_id)
        GROUP BY 
            service_name
        ORDER BY 
            Сумма DESC,
            Услуга ASC
        ''', con)
print(temp2)

# задание 3
temp3 = pd.read_sql('''
        WITH guest_service 
        AS ( 
              SELECT 
                guest_name as Фамилия_И_О,
                room_booking.room_id as room_id,
                service_booking.price as price
              FROM 
                guest
                JOIN room_booking USING (guest_id)
                JOIN service_booking USING (room_booking_id)
              WHERE
                room_booking.status_id != 3
        ),
        get_guest_stat
        AS (
            SELECT
                Фамилия_И_О,
                SUM(price) AS Сумма_за_услуги,
                COUNT(room_id) AS Количество_заселений
            FROM
                guest_service
            GROUP BY
                 Фамилия_И_О
            ORDER BY
                Фамилия_И_О ASC
        ),
        get_max_sum(max_sum)
        AS (
            SELECT
                MAX(Сумма_за_услуги)
            FROM
                get_guest_stat
        )
        
        SELECT
            Фамилия_И_О,
            Сумма_за_услуги,
            Количество_заселений
        FROM
            get_guest_stat 
            JOIN get_max_sum ON max_sum = Сумма_за_услуги
''', con)
print(temp3)

# задание 4
con.executescript('''
    
    
    CREATE TABLE bill AS 
        WITH info_tables
        AS ( 
            SELECT
                *
            FROM
                guest
                JOIN room_booking USING (guest_id)
                JOIN room USING (room_id)
                JOIN service_booking USING (room_booking_id)
         ),
        get_service_with_date(service_name, date, price)
        AS ( 
            SELECT
                service_name,
                GROUP_CONCAT(service_start_date),
                SUM(price)
            FROM
                info_tables
                JOIN service USING (service_id)
            WHERE
                guest_name = "Астахов И.И." AND
                room_name = "С-0206" AND
                check_in_date = "2021-01-13"
            GROUP BY
                service_name
         ),
        total_price(total)
        AS (
            SELECT 
                SUM(price) AS total_price 
            FROM 
                get_service_with_date
        )
        
        SELECT 
            SUBSTRING(guest_name, 1, INSTR(guest_name, " ") - 1) || " " || room_name || " " || check_in_date || "/" || check_out_date || " " || 15000
            
        FROM
            guest
            JOIN room_booking USING (guest_id)
            JOIN room USING (room_id)
        WHERE
            guest_name = "Астахов И.И." AND
            room_name = "С-0206" AND
            check_in_date = "2021-01-13"
        UNION ALL
        SELECT 
            F.service_name || " " || F.date || " " || F.price
        FROM
            get_service_with_date F
        UNION ALL
        SELECT
            CASE
                WHEN total < 15000 
                    THEN "Вернуть " || (15000 - total)
                WHEN total > 15000 
                    THEN "Доплатить " || (total - 15000)
                ELSE "Итого " || total
            END
        FROM total_price;

''')
pd.set_option('display.expand_frame_repr', False)  # Отключает перенос строк
pd.set_option('display.large_repr', 'truncate')    # Устанавливает режим обрезания для больших DataFrame

# задание 5
test5 = pd.read_sql('''
    WITH MonthlyServices AS (
    SELECT
        strftime('%m', service_start_date) AS month_num,
        service_start_date,
        service_name,
        price
    FROM
        service_booking
        JOIN service USING (service_id)
    WHERE
        strftime('%Y', service_start_date) = '2020'
)

SELECT
    CASE month_num
        WHEN '01' THEN 'Январь'
        WHEN '02' THEN 'Февраль'
        WHEN '03' THEN 'Март'
        WHEN '04' THEN 'Апрель'
        WHEN '05' THEN 'Май'
        WHEN '06' THEN 'Июнь'
        WHEN '07' THEN 'Июль'
        WHEN '08' THEN 'Август'
        WHEN '09' THEN 'Сентябрь'
        WHEN '10' THEN 'Октябрь'
        WHEN '11' THEN 'Ноябрь'
        WHEN '12' THEN 'Декабрь'
    END AS Месяц,
    service_start_date AS Дата,
    service_name AS Услуга,
    price AS Сумма,
    SUM(price) OVER(PARTITION BY month_num ORDER BY service_start_date, service_name) AS Сумма_с_накоплением
FROM
    MonthlyServices
ORDER BY
    service_start_date ASC, 
    service_name ASC;
''', con)
print(test5)

con.close()
