import logging
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.models import Variable

logger = logging.getLogger(__name__)


def load_taxi_model(**context):
    """
    โหลดข้อมูลแท็กซี่ที่แปลงแล้วเข้าสู่ฐานข้อมูล MySQL ในรูปแบบ Star Schema

    ฟังก์ชันนี้ออกแบบมาเพื่อใช้กับ PythonOperator ของ Airflow

    ขั้นตอน:
        1. ดึงเส้นทางไฟล์ CSV ที่แปลงแล้วจาก XCom
        2. โหลดข้อมูลด้วย pandas
        3. เชื่อมต่อ MySQL ผ่าน SQLAlchemy โดยใช้ข้อมูลรับรองจาก Airflow Variables
        4. สร้างและเติมข้อมูลตาราง Star Schema:
           - dim_time: ตารางมิติเวลา (ชั่วโมง/วัน/วันหยุดสุดสัปดาห์)
           - dim_payment: ตารางมิติประเภทการชำระเงิน
           - fact_trips: ตารางข้อเท็จจริง (1 แถว = 1 เที่ยว) พร้อม FK ไปยังมิติต่างๆ
        5. ใช้ pandas to_sql พร้อม if_exists='replace' และ chunksize=1000
        6. บันทึก log จำนวนแถวของแต่ละตารางหลังโหลดเสร็จ
    """

    # =========================================================================
    # ขั้นตอนที่ 1: ดึงเส้นทางไฟล์ CSV ที่แปลงแล้วจาก XCom
    # =========================================================================
    ti = context["ti"]
    transformed_path = ti.xcom_pull(
        task_ids="transform_taxi_data", key="transformed_path"
    )

    if transformed_path is None:
        raise ValueError(
            "ไม่พบ transformed_path ใน XCom สำหรับ task_id='transform_taxi_data' "
            "กรุณาตรวจสอบว่า task ต้นทางได้ push ค่าด้วย key='transformed_path' แล้ว"
        )

    logger.info("ดึงเส้นทางไฟล์ที่แปลงแล้วจาก XCom: %s", transformed_path)

    # =========================================================================
    # ขั้นตอนที่ 2: โหลดข้อมูลด้วย pandas
    # =========================================================================
    df = pd.read_csv(
        transformed_path,
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    )

    logger.info("โหลดข้อมูลที่แปลงแล้วสำเร็จ จำนวน %d แถว", len(df))

    # =========================================================================
    # ขั้นตอนที่ 3: เชื่อมต่อ MySQL ผ่าน SQLAlchemy
    # =========================================================================
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_user = Variable.get("MYSQL_USER")
    mysql_pass = Variable.get("MYSQL_PASS")
    mysql_db = Variable.get("MYSQL_DB")

    connection_string = (
        f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
        "?charset=utf8mb4"
    )

    engine = create_engine(connection_string, pool_pre_ping=True)

    logger.info(
        "เชื่อมต่อ MySQL สำเร็จ — host=%s, db=%s", mysql_host, mysql_db
    )

    # =========================================================================
    # ขั้นตอนที่ 4: สร้างและเติมข้อมูลตาราง Star Schema
    # =========================================================================

    # -----------------------------------------------------------------
    # 4a  dim_time — ตารางมิติเวลา
    #     คีย์ธรรมชาติ: (pickup_hour, pickup_day_of_week)
    # -----------------------------------------------------------------
    dim_time = (
        df[["pickup_hour", "pickup_day_of_week", "is_weekend"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # สร้าง surrogate key สำหรับมิติเวลา
    dim_time.index.name = "time_id"
    dim_time = dim_time.reset_index()

    # แมปชื่อวันในสัปดาห์เพื่อให้อ่านง่ายขึ้น
    day_name_map = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    dim_time["day_name"] = dim_time["pickup_day_of_week"].map(day_name_map)

    # แมปช่วงเวลาของวัน
    def _get_time_period(hour: int) -> str:
        """แบ่งช่วงเวลาของวันจากชั่วโมง"""
        if 5 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 17:
            return "Afternoon"
        elif 17 <= hour < 21:
            return "Evening"
        else:
            return "Night"

    dim_time["time_period"] = dim_time["pickup_hour"].apply(_get_time_period)

    logger.info("สร้าง dim_time สำเร็จ — %d แถว (ชุดค่าชั่วโมง/วันที่ไม่ซ้ำกัน)", len(dim_time))

    # -----------------------------------------------------------------
    # 4b  dim_payment — ตารางมิติประเภทการชำระเงิน
    # -----------------------------------------------------------------
    payment_label_map = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    unique_payment_types = sorted(df["payment_type"].dropna().unique())

    dim_payment = pd.DataFrame(
        {
            "payment_type_id": range(len(unique_payment_types)),
            "payment_type": unique_payment_types,
            "payment_description": [
                payment_label_map.get(int(pt), "Other")
                for pt in unique_payment_types
            ],
        }
    )

    logger.info("สร้าง dim_payment สำเร็จ — %d แถว", len(dim_payment))

    # -----------------------------------------------------------------
    # 4c  สร้าง lookup dictionaries เพื่อ map FK กลับไปที่ fact table
    # -----------------------------------------------------------------
    # time_id lookup: (pickup_hour, pickup_day_of_week) → time_id
    time_lookup = (
        dim_time.set_index(["pickup_hour", "pickup_day_of_week"])["time_id"]
        .to_dict()
    )

    # payment_type_id lookup: payment_type → payment_type_id
    payment_lookup = (
        dim_payment.set_index("payment_type")["payment_type_id"].to_dict()
    )

    # -----------------------------------------------------------------
    # 4d  fact_trips — ตารางข้อเท็จจริง
    # -----------------------------------------------------------------
    fact_trips = df.copy()

    # แมป foreign keys
    fact_trips["time_id"] = fact_trips.apply(
        lambda row: time_lookup.get(
            (row["pickup_hour"], row["pickup_day_of_week"])
        ),
        axis=1,
    )

    fact_trips["payment_type_id"] = fact_trips["payment_type"].map(payment_lookup)

    # เลือกเฉพาะคอลัมน์ที่ต้องการสำหรับ fact table
    fact_columns = [
        "time_id",
        "payment_type_id",
        "fare_amount",
        "trip_distance",
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile",
        "passenger_count",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]

    # กรองเฉพาะคอลัมน์ที่มีอยู่จริงใน DataFrame
    available_fact_columns = [c for c in fact_columns if c in fact_trips.columns]
    fact_trips = fact_trips[available_fact_columns].reset_index(drop=True)

    # สร้าง surrogate key สำหรับ fact table
    fact_trips.index.name = "trip_id"
    fact_trips = fact_trips.reset_index()

    logger.info("สร้าง fact_trips สำเร็จ — %d แถว", len(fact_trips))

    # =========================================================================
    # ขั้นตอนที่ 5: เขียนลง MySQL ด้วย to_sql (if_exists='replace', chunksize=1000)
    # =========================================================================
    logger.info("กำลังเขียน dim_time ลง MySQL...")
    dim_time.to_sql(
        name="dim_time",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.info("กำลังเขียน dim_payment ลง MySQL...")
    dim_payment.to_sql(
        name="dim_payment",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.info("กำลังเขียน fact_trips ลง MySQL...")
    fact_trips.to_sql(
        name="fact_trips",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.info("เขียนข้อมูลลง MySQL สำเร็จทั้ง 3 ตาราง")

    # =========================================================================
    # ขั้นตอนที่ 6: บันทึก log จำนวนแถวของแต่ละตารางหลังโหลดเสร็จ
    # =========================================================================
    with engine.connect() as conn:
        for table_name in ["dim_time", "dim_payment", "fact_trips"]:
            result = conn.execute(
                text(f"SELECT COUNT(*) AS cnt FROM {table_name}")
            )
            row_count = result.scalar()
            logger.info(
                "ตาราง %-15s → จำนวนแถวในฐานข้อมูล: %d", table_name, row_count
            )

    # ปิด engine อย่างสะอาด
    engine.dispose()
    logger.info("ปิดการเชื่อมต่อ MySQL เรียบร้อย — โหลดข้อมูลเสร็จสมบูรณ์")

    # Push ข้อมูลสรุปไปยัง XCom สำหรับ task ถัดไป (ถ้ามี)
    load_summary = {
        "dim_time_rows": len(dim_time),
        "dim_payment_rows": len(dim_payment),
        "fact_trips_rows": len(fact_trips),
    }
    ti.xcom_push(key="load_summary", value=load_summary)

    return load_summary