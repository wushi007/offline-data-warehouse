"""本机 ETL 运行记录与 SQL 异常查看页。"""
import json
from pathlib import Path
import subprocess
from urllib.parse import quote
from datetime import datetime
from zoneinfo import ZoneInfo

from html import escape
import math
import streamlit as st
from store import connect
from parser import decode_log

st.set_page_config(page_title='ETL 运行台',page_icon='🔎',layout='wide')
st.markdown('''<style>
.stApp {background:#f5f7fb;color:#23364b}
.block-container {padding-top:2rem;max-width:1600px}
h1,h2,h3 {font-family:"Noto Sans SC","Microsoft YaHei",sans-serif;color:#173f65}
[data-testid="stMetric"] {border-left:3px solid #3875a8;padding-left:16px}
[data-testid="stSidebar"] {background:#eaf0f8}
[data-testid="stCode"] {border-left:3px solid #ba4051}
button[kind="primary"] {background:#245f9e!important;border-color:#245f9e!important;color:white!important}
[data-testid="stVerticalBlockBorderWrapper"]:has(.record-selected) {border-color:#3875a8!important;background:#edf4fc!important}
.record-task {font-weight:650;font-size:15px;color:#173f65;overflow-wrap:anywhere;margin:0 0 4px}
.record-meta {font-size:12px;line-height:1.65;color:#64748b;overflow-wrap:anywhere}
.record-run {font-size:12px;line-height:1.6;color:#475569;overflow-wrap:anywhere;margin:5px 0}
.record-status {display:inline-block;font-size:12px;font-weight:600;border-radius:5px;padding:3px 8px;background:#e8edf3;color:#526173}
.record-status.success {background:#e3f3ed;color:#16634c}
.record-status.failed {background:#fbe7eb;color:#a42640}
.record-status.running,.record-status.queued,.record-status.up_for_retry {background:#e1edfc;color:#245f9e}
.record-error {color:#a42640;font-size:12px;overflow-wrap:anywhere;margin-top:4px}
.record-selected {font-size:12px;color:#245f9e;font-weight:600}
[data-testid="stVerticalBlockBorderWrapper"] {background:#fff}
@media (max-width:900px) { [data-testid="stHorizontalBlock"] {flex-wrap:wrap} }

</style>''',unsafe_allow_html=True)
STATUS={'success':'成功','failed':'失败','running':'运行中','queued':'排队中',
        'scheduled':'待调度','upstream_failed':'上游失败','up_for_retry':'等待重试',
        'skipped':'已跳过','unknown':'历史状态未知','None':'尚未运行',None:'尚未运行'}


def time_label(value):
    if not value:
        return '—'
    try:
        return datetime.fromisoformat(value).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return value


with st.sidebar:
    st.title('ETL 运行台')
    st.caption('任务记录与 SQL 错误')
    st.markdown('每次执行、每次重试，都有独立记录。')
    if st.button('立即同步',width='stretch'):
        try:
            result=subprocess.run(['bash',str(Path(__file__).with_name('collect_once.sh'))],
                capture_output=True,text=True,timeout=90)
            if result.returncode:
                st.error('同步失败，现有记录仍可查看。请检查采集日志。')
            else:
                st.success('同步完成')
        except subprocess.TimeoutExpired:
            st.error('同步超时，请检查 Airflow 元数据库连接。')
    st.caption('后台每 30 秒采集；页面每 30 秒刷新。时间显示为北京时间。')
    st.link_button('打开 Airflow','http://localhost:8090')


@st.fragment(run_every='30s')
def dashboard():
    with connect() as db:
        rows=[dict(r) for r in db.execute('SELECT * FROM attempts ORDER BY COALESCE(start_date,"") DESC,dag_id,run_id,task_id,attempt DESC')]
        sync=db.execute('SELECT * FROM sync_status WHERE id=1').fetchone()
    st.title('任务执行与 SQL 异常')
    st.caption('从运行结果定位到具体 SQL、脚本和日志。')
    if sync and sync['error']:
        st.warning('最近一次采集失败：'+sync['error'])
    st.caption('最近成功同步：'+time_label(sync['last_success'] if sync else None))
    if not rows:
        st.info('还没有执行记录。点击“立即同步”，或等待后台首次采集。')
        return
    for r in rows:
        r['error_items']=json.loads(r['errors'])
    a,b,c,d=st.columns(4)
    a.metric('执行尝试',len(rows))
    b.metric('成功',sum(r['state']=='success' for r in rows))
    c.metric('失败',sum(r['state']=='failed' for r in rows))
    d.metric('含 SQL 错误',sum(bool(r['error_items']) for r in rows))
    a,b,c=st.columns([2,1,1])
    dag=a.selectbox('DAG',['全部']+sorted({r['dag_id'] for r in rows}))
    states=sorted({r['state'] for r in rows if r['state']})
    state=b.selectbox('状态',['全部']+states,format_func=lambda x:STATUS.get(x,x))
    kind=c.selectbox('错误范围',['全部记录','包含 SQL 错误','仅语法错误'])
    query=st.text_input('搜索任务、运行批次或错误码',placeholder='例如 transform_orders、MERGE_CARDINALITY_VIOLATION')
    selected=[]
    for r in rows:
        if dag!='全部' and r['dag_id']!=dag: continue
        if state!='全部' and r['state']!=state: continue
        if kind=='包含 SQL 错误' and not r['error_items']: continue
        if kind=='仅语法错误' and not any(e['category']=='语法错误' for e in r['error_items']): continue
        if query and query.lower() not in ' '.join(str(r[k]) for k in ('dag_id','run_id','task_id','errors')).lower(): continue
        selected.append(r)
    st.subheader('执行记录')
    st.caption(f'共 {len(selected)} 条匹配记录 · 按执行时间倒序 · 点击“查看详情”查看 SQL 和日志')
    if not selected:
        st.info('没有匹配的记录，请调整筛选条件。')
        return
    # 筛选条件变化时回到第一页；后台刷新不改变已经选中的执行记录。
    signature=(dag,state,kind,query)
    if st.session_state.get('records_filter')!=signature:
        st.session_state['records_page']=1
        st.session_state['records_filter']=signature
    page_size=8
    pages=max(1,math.ceil(len(selected)/page_size))
    st.session_state['records_page']=min(st.session_state.get('records_page',1),pages)
    list_col,detail_col=st.columns([1.05,1.3],gap='large')
    with list_col:
        nav,summary=st.columns([1,2])
        page=int(nav.number_input('页码',min_value=1,max_value=pages,step=1,key='records_page'))
        first=(page-1)*page_size
        summary.caption(f'第 {first+1}–{min(first+page_size,len(selected))} 条 / 共 {len(selected)} 条\n共 {pages} 页，每页 {page_size} 条')
        visible=selected[first:first+page_size]
        chosen=st.session_state.get('selected_attempt_id')
        if not any(item['id']==chosen for item in selected):
            chosen=visible[0]['id']
            st.session_state['selected_attempt_id']=chosen
        for item in visible:
            active=item['id']==chosen
            with st.container(border=True):
                content,action=st.columns([3.5,1.3],gap='small')
                with content:
                    status=STATUS.get(item['state'],str(item['state'] or '尚未运行'))
                    state_class=item['state'] if item['state'] in ('success','failed','running','queued','up_for_retry') else ''
                    st.markdown(f'<div class="record-task">{escape(item["task_id"])}</div>'
                                f'<span class="record-status {state_class}">{escape(status)}</span> '
                                f'<span class="record-meta">第 {item["attempt"]} 次尝试</span>',unsafe_allow_html=True)
                    st.markdown(f'<div class="record-meta">{escape(item["dag_id"])}</div>'
                                f'<div class="record-run">批次：{escape(item["run_id"])}</div>',unsafe_allow_html=True)
                    duration=f'{item["duration"]:.1f} 秒' if item['duration'] is not None else '—'
                    mapping=f' · 映射 {item["map_index"]}' if item['map_index']>=0 else ''
                    st.markdown(f'<div class="record-meta">{escape(time_label(item["start_date"]))} · {duration}{mapping}</div>',unsafe_allow_html=True)
                    if item['error_items']:
                        codes='、'.join(dict.fromkeys(e['error_code'] or e['exception_type'] for e in item['error_items']))
                        st.markdown(f'<div class="record-error">SQL 错误：{escape(codes)}</div>',unsafe_allow_html=True)
                with action:
                    if active:
                        st.markdown('<div class="record-selected">正在查看</div>',unsafe_allow_html=True)
                    if st.button('查看详情',key='view_'+item['id'],type='primary' if active else 'secondary',width='stretch'):
                        st.session_state['selected_attempt_id']=item['id']
                        st.rerun(scope='fragment')
        st.caption('第 0 次表示尚未执行；“历史状态未知”表示只保留了日志。')
    r=next(item for item in selected if item['id']==st.session_state['selected_attempt_id'])
    with detail_col:
        st.subheader(r['task_id'])
        st.caption(f"{r['dag_id']} / {r['run_id']} / attempt={r['attempt']} / map_index={r['map_index']}")
        a,b,c=st.columns(3)
        a.write('状态：'+STATUS.get(r['state'],str(r['state'])))
        b.write('开始：'+time_label(r['start_date']))
        c.write('结束：'+time_label(r['end_date']))
        url=f"http://localhost:8090/dags/{quote(r['dag_id'],safe='')}/runs/{quote(r['run_id'],safe='')}/tasks/{quote(r['task_id'],safe='')}?try_number={r['attempt']}&map_index={r['map_index']}"
        st.link_button('在 Airflow 中查看任务',url)
        detail,logs=st.tabs(['SQL 错误详情','完整任务日志'])
        with detail:
            if not r['error_items']:
                if r['state'] in ('upstream_failed','skipped'):
                    st.info('该任务未执行，查看上游失败任务的错误详情。')
                elif r['state']=='success':
                    st.success('此任务成功，未提取到 SQL 异常。')
                else:
                    st.info('未识别到 SQL 异常。失败可能来自连接、资源、Python 代码等问题，请查看完整任务日志。')
            for number,e in enumerate(r['error_items'],1):
                st.markdown(f"**{number}. {e['category']} · {e['error_code'] or e['exception_type']}**")
                st.text(e['message'])
                if e['source_file']:
                    st.text(f"代码位置：{e['source_file']}:{e['source_line']}")
                if e['sql_line'] is not None:
                    st.text(f"SQL 位置：第 {e['sql_line']} 行，pos {e['sql_pos']}（从 0 计数）")
                if e['sql']:
                    st.code(e['sql'],language='sql',line_numbers=True)
                with st.expander('完整异常堆栈',expanded=True):
                    st.code(e['raw_traceback'],language='text')
            st.download_button('下载本次执行记录',json.dumps({k:v for k,v in r.items() if k not in ('raw_log','errors')},ensure_ascii=False,indent=2),
                               file_name='etl-attempt.json',mime='application/json')
        with logs:
            if r['raw_log']:
                st.code('\n'.join(decode_log(r['raw_log'])),language='text')
                st.download_button('下载原始日志',r['raw_log'],file_name='airflow-task.log')
            else:
                st.info('没有可读取的本地日志：任务可能尚未执行、日志已清理，或使用远端日志存储。')


dashboard()
