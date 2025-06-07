"""
Cortex Analyst App 
====================
このアプリケーションは、自然言語を使用してデータと対話することができます。
"""

import json  # JSONデータを処理するためのライブラリ
import time
from typing import Dict, List, Optional, Tuple

import _snowflake  # Snowflake固有のAPIとの連携用
import pandas as pd
import streamlit as st  # Webアプリケーション構築用のStreamlitライブラリ
from snowflake.snowpark.context import (
    get_active_session,
)  # Snowflakeセッションとの連携用
from snowflake.snowpark.exceptions import SnowparkSQLException

# 使用可能なセマンティックモデルパスのリスト（形式: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>）
# 各パスはセマンティックモデルを定義するYAMLファイルを指します
AVAILABLE_SEMANTIC_MODELS_PATHS = [
# Replace variables with your environment.
# File created by semantic-model-generator-main-local
# example "CORTEX_ANALYST_LOCAL.cortex_analyst.RAW_DATA/[your_semantic_model_file_1.yaml"
    "[YOUR_DATABASE].cortex_analyst.RAW_DATA/[your_semantic_model_file_1.yaml",
    "[YOUR_DATABASE].cortex_analyst.RAW_DATA/[your_semantic_model_file_2.yaml"    
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 500000  # ミリ秒単位（10倍に拡大）

# クエリ実行用のSnowparkセッションを初期化
session = get_active_session()

# サジェスト用の質問例（日本語）
SUGGESTED_QUESTIONS = [
    "このデータではどのような質問ができますか？",
    "データの概要を教えてください",
    "最新のトレンドを分析してください"
]


def translate_to_japanese(text: str) -> str:
    """
    Snowflake TRANSLATE関数を使用してテキストを日本語に翻訳
    
    Args:
        text (str): 翻訳対象のテキスト
        
    Returns:
        str: 翻訳されたテキスト
    """
    try:
        # テキストが既に日本語の場合はそのまま返す
        if contains_japanese(text):
            return text
            
        # TRANSLATE関数を使用して英語から日本語に翻訳
        query = f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE(
            '{text.replace("'", "''")}',  -- シングルクォートをエスケープ
            'en',  -- 元の言語（英語）
            'ja'   -- 翻訳先の言語（日本語）
        ) as translated_text
        """
        
        result = session.sql(query).collect()
        if result and len(result) > 0:
            return result[0]['TRANSLATED_TEXT']
        else:
            return text  # 翻訳に失敗した場合は元のテキストを返す
            
    except Exception as e:
        st.error(f"翻訳エラー: {str(e)}")
        return text  # エラーの場合は元のテキストを返す


def contains_japanese(text: str) -> bool:
    """
    テキストに日本語が含まれているかチェック
    
    Args:
        text (str): チェック対象のテキスト
        
    Returns:
        bool: 日本語が含まれている場合True
    """
    # ひらがな、カタカナ、漢字の範囲をチェック
    for char in text:
        if (
            '\u3040' <= char <= '\u309F' or  # ひらがな
            '\u30A0' <= char <= '\u30FF' or  # カタカナ
            '\u4E00' <= char <= '\u9FAF'     # 漢字
        ):
            return True
    return False


def translate_message_content(content: List[Dict]) -> List[Dict]:
    """
    メッセージコンテンツを日本語に翻訳
    
    Args:
        content (List[Dict]): メッセージコンテンツ
        
    Returns:
        List[Dict]: 翻訳されたメッセージコンテンツ
    """
    if not st.session_state.get("japanese_response", True):
        return content
    
    translated_content = []
    
    for item in content:
        if item["type"] == "text":
            # テキストを翻訳
            translated_text = translate_to_japanese(item["text"])
            translated_content.append({
                "type": "text",
                "text": translated_text
            })
        elif item["type"] == "suggestions":
            # サジェストを翻訳
            translated_suggestions = []
            for suggestion in item["suggestions"]:
                translated_suggestion = translate_to_japanese(suggestion)
                translated_suggestions.append(translated_suggestion)
            
            translated_content.append({
                "type": "suggestions",
                "suggestions": translated_suggestions
            })
        else:
            # その他のタイプはそのまま保持
            translated_content.append(item)
    
    return translated_content


def main():
    # セッション状態を初期化
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    if len(st.session_state.messages) == 0:
        # 初回アクセス時にサジェスト質問を表示
        display_initial_suggestions()
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()


def reset_session_state():
    """重要なセッション状態要素をリセット"""
    st.session_state.messages = []  # 会話メッセージを保存するリスト
    st.session_state.active_suggestion = None  # 現在選択されているサジェスト


def show_header_and_sidebar():
    """アプリのヘッダーとサイドバーを表示"""
    # アプリのタイトルと紹介文を設定
    st.title("Cortex Analyst")
    st.markdown(
        "Cortex Analystへようこそ！下記にご質問を入力して、データと対話してください。"
    )

    # リセットボタン付きのサイドバー
    with st.sidebar:
        st.selectbox(
            "選択されたセマンティックモデル:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        
        # 日本語応答の有効/無効を選択
        st.checkbox(
            "日本語で回答",
            value=True,
            key="japanese_response",
            help="チェックを入れると、アナリストが日本語で回答します（Snowflake TRANSLATE関数を使用）"
        )
        st.divider()
        
        # ボタンを中央配置
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("チャット履歴をクリア", use_container_width=True):
            reset_session_state()


def display_initial_suggestions():
    """初回表示時のサジェスト質問を表示"""
    st.subheader("💡 こんな質問ができます")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button(SUGGESTED_QUESTIONS[0], key="initial_suggestion_1", use_container_width=True):
            process_user_input(SUGGESTED_QUESTIONS[0])
    
    with col2:
        if st.button(SUGGESTED_QUESTIONS[1], key="initial_suggestion_2", use_container_width=True):
            process_user_input(SUGGESTED_QUESTIONS[1])
    
    with col3:
        if st.button(SUGGESTED_QUESTIONS[2], key="initial_suggestion_3", use_container_width=True):
            process_user_input(SUGGESTED_QUESTIONS[2])


def handle_user_inputs():
    """チャットインターフェースからのユーザー入力を処理"""
    # チャット入力を処理
    user_input = st.chat_input("ご質問をどうぞ")
    if user_input:
        process_user_input(user_input)
    # サジェスト質問のクリックを処理
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    """エラー通知を処理"""
    if st.session_state.get("fire_API_error_notify"):
        st.toast("APIエラーが発生しました！", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def enhance_user_prompt(prompt: str) -> str:
    """
    ユーザーのプロンプトに日本語応答の指示を追加
    
    Args:
        prompt (str): 元のユーザープロンプト
        
    Returns:
        str: 拡張されたプロンプト
    """
    if not st.session_state.get("japanese_response", True):
        return prompt
    
    # 日本語での回答を要求する指示を追加
    japanese_instruction = """

【重要】以下の点に従って回答してください：
1. 回答は必ず日本語で行ってください
2. データ分析の結果や説明は日本語で分かりやすく記述してください
3. 数値や統計情報には適切な日本語の説明を添えてください
4. 提案やインサイトも日本語で提供してください
5. 専門用語は日本語で説明してください

質問: """
    
    return japanese_instruction + prompt


def process_user_input(prompt: str):
    """
    ユーザー入力を処理し、会話履歴を更新

    Args:
        prompt (str): ユーザーの入力
    """
    # 日本語応答のためにプロンプトを拡張
    enhanced_prompt = enhance_user_prompt(prompt)
    
    # 新しいメッセージを作成（表示用は元のプロンプト、API用は拡張プロンプト）
    display_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],  # 表示用は元のプロンプト
    }
    
    api_message = {
        "role": "user", 
        "content": [{"type": "text", "text": enhanced_prompt}],  # API用は拡張プロンプト
    }
    
    # 表示用メッセージを履歴に追加
    st.session_state.messages.append(display_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message_content(display_message["content"], user_msg_index)

    # アナリストの応答を待つ間、アナリストのチャットメッセージ内にプログレス表示
    with st.chat_message("analyst"):
        with st.spinner("アナリストからの応答を待っています..."):
            time.sleep(1)
            # API呼び出し用にメッセージを準備（最後のメッセージを拡張版に置き換え）
            api_messages = st.session_state.messages[:-1] + [api_message]
            response, error_msg = get_analyst_response(api_messages)
            
            if error_msg is None:
                # 日本語翻訳が有効な場合はコンテンツを翻訳
                content = response["message"]["content"]
                if st.session_state.get("japanese_response", True):
                    content = translate_message_content(content)
                
                analyst_message = {
                    "role": "analyst",
                    "content": content,
                    "request_id": response["request_id"],
                }
            else:
                # エラーメッセージも翻訳
                error_text = error_msg
                if st.session_state.get("japanese_response", True):
                    error_text = translate_to_japanese(error_msg)
                
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_text}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True
            st.session_state.messages.append(analyst_message)
            st.rerun()


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    チャット履歴をCortex Analyst APIに送信し、応答を返す

    Args:
        messages (List[Dict]): 会話履歴

    Returns:
        Tuple[Dict, Optional[str]]: Cortex Analyst APIからの応答とエラーメッセージ
    """
    # リクエストボディをユーザーのプロンプトで準備
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
        "max_tokens": 40000,  # トークン上限を10倍程度に拡大
    }

    # Cortex Analyst APIエンドポイントにPOSTリクエストを送信
    resp = _snowflake.send_snow_api_request(
        "POST",  # メソッド
        API_ENDPOINT,  # パス
        {},  # ヘッダー
        {},  # パラメータ
        request_body,  # ボディ
        None,  # request_guid
        API_TIMEOUT,  # タイムアウト（ミリ秒）
    )

    # コンテンツはシリアライズされたJSONオブジェクトの文字列
    parsed_content = json.loads(resp["content"])

    # 応答が成功かどうかを確認
    if resp["status"] < 400:
        # 応答のコンテンツをJSONオブジェクトとして返す
        return parsed_content, None
    else:
        # 読みやすいエラーメッセージを作成
        error_msg = f"""
🚨 アナリストAPIエラーが発生しました 🚨

* レスポンスコード: `{resp['status']}`
* リクエストID: `{parsed_content.get('request_id', 'N/A')}`
* エラーコード: `{parsed_content.get('error_code', 'N/A')}`

メッセージ:
{parsed_content.get('message', '詳細不明')}

        """
        return parsed_content, error_msg


def display_conversation():
    """
    ユーザーとアシスタント間の会話履歴を表示
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_message_content(content, idx)


def display_message_content(content: List[Dict[str, str]], message_index: int):
    """
    単一のメッセージコンテンツを表示

    Args:
        content (List[Dict[str, str]]): メッセージコンテンツ
        message_index (int): メッセージのインデックス
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # サジェストをボタンとして表示
            st.subheader("💡 関連する質問")
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}",
                    use_container_width=True
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # SQLクエリと結果を表示
            display_sql_query(item["statement"], message_index)
        else:
            # 必要に応じて他のコンテンツタイプを処理
            pass


@st.cache_data(show_spinner=False, max_entries=1000)  # キャッシュサイズも拡大
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    SQLクエリを実行し、結果をpandas DataFrameに変換

    Args:
        query (str): SQLクエリ

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: クエリ結果とエラーメッセージ
    """
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int):
    """
    SQLクエリを実行し、データフレームとチャートの形で結果を表示

    Args:
        sql (str): SQLクエリ
        message_index (int): メッセージのインデックス
    """

    # SQLクエリを表示
    with st.expander("SQLクエリ", expanded=False):
        st.code(sql, language="sql")

    # SQLクエリの結果を表示
    with st.expander("結果", expanded=True):
        with st.spinner("SQLを実行中..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"生成されたSQLクエリを実行できませんでした。エラー: {err_msg}")
                return

            if df.empty:
                st.write("クエリはデータを返しませんでした")
                return

            # クエリ結果を2つのタブで表示
            data_tab, chart_tab = st.tabs(["データ 📄", "チャート 📈"])
            with data_tab:
                st.dataframe(df, use_container_width=True)

            with chart_tab:
                display_charts_tab(df, message_index)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """
    チャートタブを表示

    Args:
        df (pd.DataFrame): クエリ結果
        message_index (int): メッセージのインデックス
    """
    # チャートを描画するには少なくとも2列必要
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X軸", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y軸",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "チャートタイプを選択",
            options=["線グラフ 📈", "棒グラフ 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "線グラフ 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "棒グラフ 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("チャートの描画には少なくとも2列が必要です")


if __name__ == "__main__":
    main()
