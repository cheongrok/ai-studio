import snowflake.connector
import streamlit as st
import pandas as pd
from openai import OpenAI
import awswrangler as wr
import boto3
import json
import re


session = boto3.Session(profile_name="prod-ai-data-team")
secret = json.loads(wr.secretsmanager.get_secret("prod/db/snowflake", boto3_session=session))
conn = snowflake.connector.connect(
    user=secret["username"],
    password=secret["password"],
    account=re.sub(".snowflakecomputing.com", "", secret["host"]),
    warehouse=secret["warehouse"],
    database=secret["database"],
    schema=secret["schema"]
)


def run_query_df(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]  # 컬럼 이름 추출
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)


def parse_json_safely(text: str) -> dict:
    """모델 응답에서 JSON만 뽑아 안전하게 dict로 변환."""
    # 1) 우선 그대로 시도
    try:
        return json.loads(text)
    except Exception:
        pass


@st.cache_data(ttl=86400)  # 24시간 = 60*60*24초
def product_review(seller_name: str) -> str:
    sql = f"""
        WITH t1 AS (
            SELECT 
                m.user_seq AS seller_seq,
                m.user_name AS seller_name,
                pr.product_seq AS product_seq,
                pi.product_id AS product_id,
                pi.product_name AS product_name,
                pi.flash AS flash,
                pi.cost_price AS cost_price,
                pr.user_seq AS user_seq,
                pr.review_seq AS review_seq,
                pr.review AS review,
                pr.ratio AS ratio,
                pr.review_length AS review_length,
                pr.created_at AS created_at_review,
                pr.seller_comment AS seller_comment,
                CONCAT('https://thumb-ssl.grip.show', ppi.image_path, '?type=w&w=150') AS image_path
            FROM grip_db_realtime.member m 
                LEFT JOIN grip_db.product_review pr ON m.user_seq = pr.seller_seq
                LEFT JOIN grip_db_realtime.product_info pi ON pr.product_seq = pi.product_seq
                LEFT JOIN grip_db_realtime.product_preview_image ppi ON ppi.product_seq = pi.product_seq
            WHERE
                m.user_name = '{seller_name}'
                AND pr.created_at > CURRENT_TIMESTAMP - INTERVAL '6 MONTH'
                AND pr.review_length > 0
                AND pi.cost_price > 0
        ),

        t2 AS (
            SELECT
                relation_seq,
                image_path AS review_image_path
            FROM grip_db_realtime.attached_image
            WHERE image_type = 14
        )

        SELECT
            t1.seller_seq,
            t1.seller_name,
            t1.product_seq,
            t1.product_id,
            t1.product_name,
            t1.cost_price,
            t1.flash,
            t1.user_seq,
            m.user_name AS user_name,
            t1.review_seq,
            t1.review,
            t1.ratio,
            t1.review_length,
            t1.created_at_review,
            t1.seller_comment,
            t1.image_path,
            t1.product_name<>t1.review,
            CONCAT('https://thumb-ssl.grip.show', t2.review_image_path, '?type=w&w=150') AS review_image_path

        FROM t1
            LEFT JOIN grip_db_realtime.member m ON m.user_seq = t1.user_seq
            LEFT JOIN t2 ON t2.relation_seq = t1.review_seq
    """
    df = run_query_df(conn, sql)
    df = df.drop_duplicates(['PRODUCT_SEQ', 'REVIEW'])
    df = df.sort_values(['REVIEW_LENGTH'], ascending=False)
    return df

@st.cache_data(ttl=86400)  # 24시간 = 60*60*24초
def flash_product_info():
    sql = """
          select fpi.live_id                live_id, \
                 c.title                    title, \
                 m.user_seq                 user_seq, \
                 m.user_name                user_name, \
                 fpi.request_at             request_at, \
                 fpi.product_name           product_name, \
                 fpi.lv2_category_name      lv2_category_name, \
                 fpi.lv3_category_name      lv3_category_name, \
                 fpi.lv4_category_name      lv4_category_name, \
                 fpi.tags                   tags, \
                 fpi.description            description, \
                 fpi.product_id             product_id, \
                 CONCAT('https://thumb-ssl.grip.show', \
                        (case when length(ppi.image_path) > 0 then ppi.image_path else pppi.image_path end), \
                        '?type=w&w=500') AS image_path, \
                 pi.cost_price              cost_price
          from aibigdata_db.flash_product_info fpi

                   left join grip_db_realtime.product_info pi on pi.product_id = fpi.product_id
                   left join grip_db_realtime.product_preview_image ppi ON ppi.product_seq = pi.product_seq
                   left join grip_db_realtime.product_preview_image pppi \
                             ON (pppi.image_seq = 1 and pppi.product_seq = pi.product_seq)
                   left join grip_db_realtime.content c on c.content_id = fpi.live_id
                   left join grip_db_realtime.member m on m.user_seq = c.user_seq

          where fpi.product_id is not null
            -- and pi.deleted = 'N'
            -- and pi.excluded = 'N'
            and pi.cost_price > 0 \
          """
    df = run_query_df(conn, sql)
    return df


@st.cache_data(ttl=86400)  # 24시간 = 60*60*24초
def always_product_info():
    sql = """
          SELECT pi.product_seq  AS product_seq, \
                 pi.product_id   AS product_id, \
                 pi.product_name AS product_name, \
                 pc.category_seq AS category_seq, \
                 c.category_name AS category_name, \
                 pi.cost_price      cost_price, \
                 CONCAT('https://thumb-ssl.grip.show', \
                        (CASE \
                             WHEN length(ppi.image_path) > 0 THEN ppi.image_path \
                             ELSE pppi.image_path \
                            END), \
                        '?type=w&w=500' \
                 )               AS image_path
          FROM grip_db_realtime.product_info pi
                   LEFT JOIN grip_db_realtime.product_preview_image ppi ON ppi.product_seq = pi.product_seq
                   LEFT JOIN grip_db.product_preview_image pppi \
                             ON (pppi.image_seq = 1 AND pppi.product_seq = pi.product_seq)
                   LEFT JOIN grip_db.product_category pc ON pi.product_seq = pc.product_seq
                   LEFT JOIN grip_db.category c ON c.category_seq = pc.category_seq
          WHERE pi.flash = 'N'
            AND pi.deleted = 'N'
            AND pi.excluded = 'N'
            AND pi.cost_price > 0
            AND pi.created_at >= DATEADD(YEAR, -1, CURRENT_TIMESTAMP) \

          """
    df = run_query_df(conn, sql)
    return df


def prep_review(words):
    words = words.replace("\n", " ")
    words = re.sub(r"[^a-zA-Z가-힣0-9\s]", " ", words)  # 특수 기호 제거
    words = re.sub(r"\s+", " ", words)  # 공백을 하나로 통일
    return words.strip()  # 문자열 양 끝의 공백 제거


st.set_page_config(layout="wide")
if __name__ == "__main__":
    st.title("🧾 포토후기")
    st.markdown("""
    - 판매자 이름을 입력하면 해당 판매자의 포토후기를 조회합니다.
    - 최근 12개월 이내 작성된 포토후기만 조회합니다.
    """)


    seller_name = st.text_input("이름을 입력해주세요.", placeholder="예: 제제시스터")
    if seller_name:

        review_df = product_review(seller_name)
        flash_df = flash_product_info()
        always_df = always_product_info()
        flash_sub_df = flash_df[[
            "PRODUCT_NAME", "LV2_CATEGORY_NAME", "LV3_CATEGORY_NAME", "LV4_CATEGORY_NAME",
            "PRODUCT_ID", "COST_PRICE"
        ]]
        flash_sub_df = flash_sub_df.rename(columns={"PRODUCT_NAME": "LLM_PRODUCT_NAME"})
        flash_sub_df = flash_sub_df.fillna("").drop_duplicates(["PRODUCT_ID"])

        always_sub_df = always_df.drop_duplicates(["PRODUCT_ID", "CATEGORY_SEQ"])
        always_sub_df = always_sub_df.groupby("PRODUCT_ID")["CATEGORY_NAME"].apply(list).reset_index(
            name="CATEGORY_LIST")

        product_df = pd.merge(always_sub_df, flash_sub_df, how="outer")

        review_sub_df = pd.merge(review_df, product_df, on="PRODUCT_ID", how="left")
        review_sub_df = review_sub_df.fillna("")
        st.dataframe(review_sub_df.head(1))
        # --- 여기부터 이미지 3열 종대 출력 ---
        # URL 정제(빈 값/공백/비URL 제거)
        urls = (
            review_sub_df["REVIEW_IMAGE_PATH"]
            .dropna()
            .astype(str)
            .map(str.strip)
            .tolist()
        )
        urls = [u for u in urls if u.startswith("http://") or u.startswith("https://")]


        # 3열 종대 이미지 그리드 표시
        if urls:
            # 행 단위(3개씩)로 끊어 배치하면 좌→우, 위→아래 순으로 깔끔하게 정렬됩니다.
            for i in range(0, len(urls), 3):
                row_urls = urls[i:i + 3]
                cols = st.columns(3, gap="small")
                for col, u in zip(cols, row_urls):
                    with col:
                        st.image(u, use_container_width=False, width=300)
        else:
            st.info("표시할 이미지가 없습니다.")

