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

secret_llm = json.loads(wr.secretsmanager.get_secret("prod/external-api-keys", boto3_session=session))
client = OpenAI(api_key=secret_llm.get("openai-api-key"))


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


def summary(user_message):
    resp = client.chat.completions.create(
        model="gpt-4.1-nano",
        messages=[
            {"role": "system", "content": "리뷰를 요약하는 유능한 마케터야."},
            {
                "role": "user",
                "content": user_message  # ✅ 문자열 그대로 전달
            },
        ],
        temperature=0.9,
        max_tokens=1000,
    )
    raw = resp.choices[0].message.content
    return raw


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

def prep_review(words):
    words = words.replace("\n", " ")
    words = re.sub(r"[^a-zA-Z가-힣0-9\s]", " ", words)  # 특수 기호 제거
    words = re.sub(r"\s+", " ", words)  # 공백을 하나로 통일
    return words.strip()  # 문자열 양 끝의 공백 제거


DEFAULT_PROMPT = """
<페르소나>
너는 이커머스에서 판매자들을 관리하는 MD야. 판매자들의 마케팅, 홍보를 도와주는 역할을 해.
</페르소나>

<문제>
다음의 <리뷰>를 참조하여 구매자들의 반응은 어떤지, 그리고 판매자에 대한 긍정적인 소개를 생성해줘.
소개는 해시태그로 나타낼거야. 3개의 해시태그로 판매자를 소개하는 문구로 결과를 출력해줘. 각 해시태그는 10자 이내로 생성해줘.
</문제>

<예시>
>'#소통이잘되는 #사이즈딱맞아요 #고퀄리티가성비'
</예시>

<리뷰>
{reviews}
</리뷰>
"""





st.set_page_config(layout="wide")
if __name__ == "__main__":
    st.title("🧾 리뷰 해시태그 생성")

    # 1) 입력부
    seller_name = st.text_input("판매자 이름을 입력하세요", placeholder="예: 제제시스터")
    user_prompt = st.text_area(
        "프롬프트를 입력하세요 (아래 기본 예시 그대로 사용하거나 수정 가능)",
        value=DEFAULT_PROMPT,
        height=220
    )

    run = st.button("해시태그 생성", type="primary", disabled=not seller_name)

    # 2) 실행부
    if run and seller_name:
        with st.spinner("리뷰를 불러오고 해시태그를 생성 중..."):
            review_df = product_review(seller_name)

            if review_df is None or review_df.empty:
                st.warning("해당 판매자의 리뷰가 없습니다.")
                st.stop()

            # 샘플링(정렬 기준은 기존 로직 유지)
            sample_df = review_df.sort_values(
                ["REVIEW_IMAGE_PATH", "REVIEW_LENGTH", "CREATED_AT_REVIEW", "COST_PRICE"],
                ascending=[False, False, False, False]
            ).head(10)

            # 모델에 전달할 리뷰 딕셔너리
            reviews = {f"{i}번째고객": text for i, text in enumerate(sample_df["REVIEW"], start=1)}

            # 3) 프롬프트 구성
            # 사용자가 {reviews} 토큰을 빼먹었을 경우를 대비한 안전장치
            final_prompt = (
                user_prompt if "{reviews}" in user_prompt
                else user_prompt + "\n\n<리뷰>\n{reviews}\n</리뷰>\n"
            )
            user_message = final_prompt.format(reviews=reviews)

            # 4) LLM 호출
            st.markdown("### 💫 해시태그 추출")
            st.write(summary(user_message))

            st.markdown("---")
            st.markdown("#### 🔎 샘플 리뷰(상위 10건)")
            st.dataframe(sample_df, use_container_width=True)

            # (옵션) 이미지 컬럼 미리보기
            # REVIEW_IMAGE_PATH 컬럼이 URL이면 섬네일로 프리뷰
            if "REVIEW_IMAGE_PATH" in sample_df.columns:
                urls = [u for u in sample_df["REVIEW_IMAGE_PATH"].tolist() if isinstance(u, str) and u]
                if urls:
                    st.markdown("#### 🖼️ 리뷰 이미지 미리보기")
                    st.image(urls, width=120, clamp=True)