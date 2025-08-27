import streamlit as st
import pandas as pd
import os
import json
import shutil
import re
import time
import numpy as np


# 工具函数：路径安全文件名
def safe_file_name(name):
    return re.sub(r'[\/:*?"<>|]', "_", os.path.splitext(name)[0]).strip()

# 初始化状态
def init_state():
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'selected_file' not in st.session_state:
        st.session_state.selected_file = None
    if 'processed_rows' not in st.session_state:
        st.session_state.processed_rows = set()
    if 'current_row' not in st.session_state:
        st.session_state.current_row = 0
    if 'deleted_rows' not in st.session_state:
        st.session_state.deleted_rows = []
    if 'working_dir' not in st.session_state:
        st.session_state.working_dir = None
    if 'dispute_count' not in st.session_state:
        st.session_state.dispute_count = {}

# rerun 兼容性处理
try:
    rerun = st.rerun
except AttributeError:
    rerun = st.experimental_rerun

# 日志记录
def append_log(action, row_index, deleted_row_data=None):
    log_path = os.path.join(st.session_state.working_dir, "log.jsonl")
    log_entry = {
        "action": action,
        "row_index": row_index,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "row_data": st.session_state.df.iloc[row_index].to_dict() if deleted_row_data is None else deleted_row_data
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    if action == "删除" and deleted_row_data is not None:
        deleted_path = os.path.join(st.session_state.working_dir, "deleted.csv")
        df_deleted = pd.DataFrame([deleted_row_data])
        if os.path.exists(deleted_path):
            df_deleted.to_csv(deleted_path, mode='a', index=False, header=False)
        else:
            df_deleted.to_csv(deleted_path, index=False)

# 保存审核进度
def save_progress(base_dir, is_inprogress=True):
    if st.session_state.df is not None:
        status = "inprogress" if is_inprogress else "audited"
        st.session_state.df.to_csv(os.path.join(base_dir, f"{status}.csv"), index=False)

        # ➕ 新增部分：实时保存一份 audited.csv（只保存已审核行）
        audited_path = os.path.join(base_dir, "audited.csv")
        audited_df = st.session_state.df.iloc[list(st.session_state.processed_rows)]
        audited_df.to_csv(audited_path, index=False)

        # 保存进度信息
        with open(os.path.join(base_dir, "progress.json"), "w", encoding="utf-8") as f:
            json.dump({
                "current_row": st.session_state.current_row,
                "processed_rows": list(st.session_state.processed_rows),
                "dispute_count": st.session_state.dispute_count
            }, f)

# 加载审核进度
def load_progress(base_dir):
    inprogress_path = os.path.join(base_dir, "inprogress.csv")
    progress_path = os.path.join(base_dir, "progress.json")

    if os.path.exists(inprogress_path):
        st.session_state.df = pd.read_csv(inprogress_path)
    else:
        original_path = os.path.join(base_dir, "original.csv")
        st.session_state.df = pd.read_csv(original_path)

    if os.path.exists(progress_path):
        with open(progress_path, encoding="utf-8") as f:
            d = json.load(f)
            st.session_state.current_row = d.get("current_row", 0)
            st.session_state.processed_rows = set(d.get("processed_rows", []))
            st.session_state.dispute_count = d.get("dispute_count", {})

# 渲染输入框（方案 2：统一 text_input，兼容空列）
def render_input(col, val, row_idx):
    if pd.isna(val):
        val = ""
    return st.text_input(f"{col}", value=str(val), key=f"text_input_{col}_{row_idx}")

# 强制类型转换
def try_cast(v, original):
    try:
        return type(original)(v)
    except:
        return v

# 查找下一个未处理的索引
def next_unprocessed_index():
    total = len(st.session_state.df)
    for i in range(st.session_state.current_row + 1, total):
        if i not in st.session_state.processed_rows:
            return i
    for i in range(0, st.session_state.current_row):
        if i not in st.session_state.processed_rows:
            return i
    return total

# 初始化
init_state()
st.title("📦 法条案由审核系统")
# 配置输入文件夹
INPUT_FOLDER = "input_data"  # 你本地手动放 CSV 文件的文件夹

# 确保目录存在
os.makedirs(INPUT_FOLDER, exist_ok=True)

# 扫描 CSV 文件
csv_files = []
for root, _, files in os.walk(INPUT_FOLDER):
    for file in files:
        if file.lower().endswith(".csv"):
            rel_path = os.path.relpath(os.path.join(root, file), INPUT_FOLDER)
            csv_files.append(rel_path)

# 显示可选 CSV
# if csv_files:
#     selected = st.selectbox("选择要审核的文件", csv_files)
# 显示可选 CSV（带搜索功能）
if csv_files:
    search_term = st.text_input("🔍 输入关键词筛选文件", "")
    filtered_files = [f for f in csv_files if search_term.lower() in f.lower()]

    if filtered_files:
        selected = st.selectbox("选择要审核的文件", filtered_files)
    else:
        st.warning("未找到匹配的文件")
        selected = None

    if selected:
        safe_name = safe_file_name(selected)
        working_dir = os.path.join("data", safe_name)
        os.makedirs(working_dir, exist_ok=True)

        if not os.path.exists(os.path.join(working_dir, "original.csv")):
            shutil.copy(os.path.join(INPUT_FOLDER, selected), os.path.join(working_dir, "original.csv"))

        st.session_state.selected_file = selected
        st.session_state.working_dir = working_dir
        st.session_state.current_row = 0
        st.session_state.processed_rows = set()
        st.session_state.deleted_rows = []
        st.session_state.dispute_count = {}

        load_progress(working_dir)
        st.success(f"已加载文件：{selected}")
else:
    st.warning("⚠️ 请将 CSV 文件放入 input_data 文件夹中")

# 审核逻辑
if st.session_state.df is not None:
    df = st.session_state.df
    idx = st.session_state.current_row

    if idx < len(df):
        row = df.iloc[idx]
        st.write(f"当前审核第 {idx + 1} / {len(df)} 行")
        st.dataframe(row.to_frame())

        # 选择操作
        op = st.radio("选择操作", ["通过", "修改", "删除"], key=f"op_radio_{idx}")

        # 修改
        if op == "修改":
            new_vals = {col: render_input(col, row[col], idx) for col in df.columns}
            if st.button("保存修改", key=f"save_mod_{idx}"):
                for c, v in new_vals.items():
                    df.at[idx, c] = try_cast(v, row[c])
                append_log("修改", idx)
                st.session_state.processed_rows.add(idx)
                st.session_state.current_row = next_unprocessed_index()
                save_progress(st.session_state.working_dir, True)
                rerun()

        # 删除
        elif op == "删除":
            if st.button("确认删除", key=f"confirm_delete_{idx}"):
                deleted_data = df.iloc[idx].to_dict()
                append_log("删除", idx, deleted_row_data=deleted_data)

                df.drop(idx, inplace=True)
                df.reset_index(drop=True, inplace=True)

                # 重新映射 processed_rows
                st.session_state.processed_rows = {
                    new_idx
                    for new_idx in range(len(df))
                    if new_idx != idx  # 被删掉
                       and new_idx in st.session_state.processed_rows
                }

                # 重新定位下一个未处理的 index
                unprocessed = set(range(len(df))) - st.session_state.processed_rows
                st.session_state.current_row = min(unprocessed) if unprocessed else len(df)

                save_progress(st.session_state.working_dir, True)
                rerun()


        # 通过
        else:
            if st.button("通过", key=f"pass_{idx}"):
                append_log("通过", idx)
                st.session_state.processed_rows.add(idx)
                st.session_state.current_row = next_unprocessed_index()
                save_progress(st.session_state.working_dir, True)
                rerun()

        st.markdown("---")
        # ===== 新增纠纷类型 =====
        st.markdown("### ➕ 新增纠纷类型")
        new_dispute_type = st.text_input("输入新纠纷类型", key=f"new_dispute_type_{idx}")

        if st.button("新增纠纷类型", key=f"add_dispute_type_{idx}"):
            # 1️⃣ 查找已有 dispute_type_ 列
            existing_cols = [col for col in df.columns if col.startswith("dispute_type_")]
            next_index = len(existing_cols) + 1
            new_col = f"dispute_type_{next_index}"

            # 2️⃣ 如果新列不存在，增加空列
            if new_col not in df.columns:
                df[new_col] = ""

            # 3️⃣ 当前行更新数据
            df.at[idx, new_col] = new_dispute_type.strip()

            # 4️⃣ 日志保存
            append_log("新增纠纷类型", idx)
            save_progress(st.session_state.working_dir, True)

            # 5️⃣ 页面刷新
            rerun()

        st.markdown("---")

    else:
        st.success("✅ 审核完成")
        if st.button("导出审核结果"):
            save_progress(st.session_state.working_dir, False)

    # 审核进度条
    st.progress(len(st.session_state.processed_rows) / max(1, len(df)))
    st.write(f"已审核 {len(st.session_state.processed_rows)} / {len(df)} 行")

    # # 审核记录支持关键词搜索 + 分页展示
    # log_path = os.path.join(st.session_state.working_dir, "log.jsonl")
    # if os.path.exists(log_path):
    #     with open(log_path, "r", encoding="utf-8") as f:
    #         logs = [json.loads(line) for line in f.readlines()]
    #
    #     if logs:
    #         # 搜索框
    #         keyword = st.text_input("🔍 输入关键词搜索（支持行号、操作、字段内容）", key="log_search_input").strip()
    #
    #         # 过滤日志
    #         if keyword:
    #             keyword_lower = keyword.lower()
    #
    #
    #             def match(log_entry):
    #                 if keyword_lower in str(log_entry["row_index"]).lower():
    #                     return True
    #                 if keyword_lower in str(log_entry["action"]).lower():
    #                     return True
    #                 if keyword_lower in str(log_entry.get("timestamp", "")).lower():
    #                     return True
    #                 row_data = log_entry.get("row_data", {})
    #                 for v in row_data.values():
    #                     if keyword_lower in str(v).lower():
    #                         return True
    #                 return False
    #
    #
    #             filtered_logs = [log for log in logs if match(log)]
    #         else:
    #             filtered_logs = logs
    #
    #         # 分页
    #         logs_per_page = 10
    #         total_pages = (len(filtered_logs) + logs_per_page - 1) // logs_per_page
    #
    #         if "log_page" not in st.session_state:
    #             st.session_state.log_page = 0
    #         # 如果搜索了，自动跳回第一页
    #         if keyword and st.session_state.log_page != 0:
    #             st.session_state.log_page = 0
    #
    #         st.markdown(
    #             f"### 📝 审核记录（共 {len(filtered_logs)} 条，当前第 {st.session_state.log_page + 1} 页 / 共 {total_pages} 页）")
    #
    #         start_idx = st.session_state.log_page * logs_per_page
    #         end_idx = min(start_idx + logs_per_page, len(filtered_logs))
    #
    #         for i in range(start_idx, end_idx):
    #             log_entry = filtered_logs[i]
    #             row_idx = log_entry["row_index"]
    #             action = log_entry["action"]
    #             timestamp = log_entry["timestamp"]
    #             st.write(f"**行号： {row_idx}，操作：{action}，时间：{timestamp}**")
    #
    #             row_data = log_entry.get("row_data")
    #             if row_data:
    #                 df_row = pd.DataFrame([row_data])
    #                 st.dataframe(df_row)
    #
    #             btn_key = f"re_audit_{i}_{row_idx}"
    #             if st.button("重新审核", key=btn_key):
    #                 if row_idx in st.session_state.processed_rows:
    #                     st.session_state.processed_rows.remove(row_idx)
    #
    #                 if action == "删除":
    #                     deleted_data = log_entry.get("row_data")
    #                     if deleted_data is not None:
    #                         df = st.session_state.df
    #                         insert_pos = min(row_idx, len(df))
    #                         restored_df = pd.DataFrame([deleted_data])
    #                         df_top = df.iloc[:insert_pos]
    #                         df_bottom = df.iloc[insert_pos:]
    #                         df = pd.concat([df_top, restored_df, df_bottom], ignore_index=True)
    #                         st.session_state.df = df
    #
    #                 st.session_state.current_row = row_idx
    #                 save_progress(st.session_state.working_dir, True)
    #                 st.rerun()
    #
    #         # 分页按钮放下面
    #         st.markdown("---")
    #         col1, col2, col3 = st.columns([1, 6, 1])
    #         with col1:
    #             if st.button("⬅ 上一页") and st.session_state.log_page > 0:
    #                 st.session_state.log_page -= 1
    #         with col3:
    #             if st.button("下一页 ➡") and st.session_state.log_page < total_pages - 1:
    #                 st.session_state.log_page += 1
    # 审核记录支持关键词搜索 + 分页展示
    log_path = os.path.join(st.session_state.working_dir, "log.jsonl")
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            logs = [json.loads(line) for line in f.readlines()]
        logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        if logs:
            keyword = st.text_input("🔍 输入关键词搜索（支持行号、操作、字段内容）", key="log_search_input").strip()

            if keyword:
                keyword_lower = keyword.lower()


                def match(log_entry):
                    if keyword_lower in str(log_entry["row_index"]).lower():
                        return True
                    if keyword_lower in str(log_entry["action"]).lower():
                        return True
                    if keyword_lower in str(log_entry.get("timestamp", "")).lower():
                        return True
                    row_data = log_entry.get("row_data", {})
                    for v in row_data.values():
                        if keyword_lower in str(v).lower():
                            return True
                    return False


                filtered_logs = [log for log in logs if match(log)]
            else:
                filtered_logs = logs

            logs_per_page = 10
            total_pages = (len(filtered_logs) + logs_per_page - 1) // logs_per_page

            if "log_page" not in st.session_state:
                st.session_state.log_page = 0
            if keyword and st.session_state.log_page != 0:
                st.session_state.log_page = 0

            st.markdown(
                f"### 📝 审核记录（共 {len(filtered_logs)} 条，当前第 {st.session_state.log_page + 1} 页 / 共 {total_pages} 页）")

            start_idx = st.session_state.log_page * logs_per_page
            end_idx = min(start_idx + logs_per_page, len(filtered_logs))

            for i in range(start_idx, end_idx):
                log_entry = filtered_logs[i]
                row_idx = log_entry["row_index"]
                action = log_entry["action"]
                timestamp = log_entry["timestamp"]

                header_text = f"【{i + 1}】行号: {row_idx}，操作: {action}，时间: {timestamp}"
                with st.expander(header_text, expanded=False):
                    row_data = log_entry.get("row_data", {})
                    clean_data = {k: ("" if (isinstance(v, float) and np.isnan(v)) else v) for k, v in row_data.items()}
                    st.json(clean_data)

                    btn_key = f"re_audit_{i}_{row_idx}"
                    if st.button("重新审核", key=btn_key):
                        if row_idx in st.session_state.processed_rows:
                            st.session_state.processed_rows.remove(row_idx)

                        if action == "删除":
                            deleted_data = log_entry.get("row_data")
                            if deleted_data is not None:
                                df = st.session_state.df
                                insert_pos = min(row_idx, len(df))
                                restored_df = pd.DataFrame([deleted_data])
                                df_top = df.iloc[:insert_pos]
                                df_bottom = df.iloc[insert_pos:]
                                df = pd.concat([df_top, restored_df, df_bottom], ignore_index=True)
                                st.session_state.df = df

                        st.session_state.current_row = row_idx
                        save_progress(st.session_state.working_dir, True)
                        st.rerun()

            # 分页按钮
            # st.markdown("---")
            # col1, col2, col3 = st.columns([1, 6, 1])
            # with col1:
            #     if st.button("上一页") and st.session_state.log_page > 0:
            #         st.session_state.log_page -= 1
            # with col3:
            #     if st.button("下一页") and st.session_state.log_page < total_pages - 1:
            #         st.session_state.log_page += 1
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 6, 1])

            with col1:
                if st.button("跳转到第一页"):
                    st.session_state.log_page = 0
                if st.button("上一页") and st.session_state.log_page > 0:
                    st.session_state.log_page -= 1

            with col3:
                if st.button("下一页") and st.session_state.log_page < total_pages - 1:
                    st.session_state.log_page += 1
                if st.button("跳转到最后一页"):
                    st.session_state.log_page = total_pages - 1

    # 下载区
    if st.session_state.df is not None:
        audited_df = st.session_state.df.iloc[list(st.session_state.processed_rows)]
        audited_csv = audited_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 下载已审核数据", audited_csv, file_name="audited_data.csv", mime='text/csv')

    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            logs_bytes = f.read().encode("utf-8")
        st.download_button("📜 下载审核日志（log.jsonl）", logs_bytes, file_name="log.jsonl", mime="application/json")

