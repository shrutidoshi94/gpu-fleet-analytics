"""Offline GPU capacity planning. All reads are from the verified local Gold bundle."""
from pathlib import Path
import os
import runpy
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from dashboard_data import (verify_bundle,read_table,select,hourly,cohort_summary,
                            safe_divide,eligible_activity,scenario)

ROOT=Path(__file__).resolve().parents[1]
COLORS=['#118a7e','#4f70d6','#e6a13d','#da6b59','#8d99ae']


def number(value,digits=0):
    return 'Unknown' if value is None or pd.isna(value) else f'{value:,.{digits}f}'


def percent(value):
    return 'Unknown' if value is None or pd.isna(value) else f'{value:.1%}'


@st.cache_resource(show_spinner='Verifying the local snapshot…')
def catalog(folder,stamp):
    return verify_bundle(folder)


@st.cache_data(max_entries=8,show_spinner=False)
def data(folder,name,identity):
    return read_table(folder,name)


@st.cache_data(max_entries=3,show_spinner=False)
def mix_data(folder,identity,dimension,lo,hi,model,cluster):
    filters=[('dimension','=',dimension),('day','>=',lo),('day','<=',hi)]
    if model is not None:filters.append(('gpu_spec_public','=',model))
    if cluster is not None:filters.append(('cluster_id','=',cluster))
    return read_table(folder,'workload_mix_hourly',filters=filters)


def line_chart(frame,fields,title,step=1):
    records=[]
    for field,label in fields.items():
        chunk=frame[['trace_hour','trace_day',field]].dropna().copy()
        chunk=chunk.sort_values('trace_hour')
        chunk['segment']=(chunk.trace_hour.diff().fillna(step)>step).cumsum().astype(str)
        chunk=chunk.rename(columns={field:'value'});chunk['Series']=label
        if not chunk.empty:records.append(chunk)
    if not records or not any(len(x) for x in records):st.info('No measured values for this selection.');return
    chart=pd.concat(records,ignore_index=True)
    st.altair_chart(alt.Chart(chart).mark_line(strokeWidth=2.2).encode(
        x=alt.X('trace_day:Q',title='Relative trace day',axis=alt.Axis(format='.0f',grid=False)),
        y=alt.Y('value:Q',title=title,scale=alt.Scale(zero=True)),
        color=alt.Color('Series:N',scale=alt.Scale(range=COLORS),legend=alt.Legend(orient='top',title=None,labelLimit=0)),
        detail=['Series:N','segment:N'],
        tooltip=[alt.Tooltip('trace_day:Q',title='Trace day',format='.2f'),'Series:N',alt.Tooltip('value:Q',format=',.2f')]
    ).properties(height=310).configure_view(stroke=None),use_container_width=True)


def csv_download(frame,label,name):
    st.download_button(label,frame.to_csv(index=False).encode(),file_name=name,mime='text/csv',use_container_width=False)


def choose_gpu():
    """Persist a chart pick before Streamlit reruns all three tabs."""
    picks=st.session_state.get('pressure_chart',{}).get('selection',{}).get('gpu',[])
    if picks and picks[0].get('GPU cohort'):
        st.session_state['focus']=picks[0]['GPU cohort']


@st.cache_resource(show_spinner='Preparing the saved Gold data…')
def hosted_bundle(spec_json, destination):
    from app.bundle_setup import install_bundle
    return install_bundle(json.loads(spec_json), destination)


def main():
    folder=Path(os.environ.get('GOLD_BUNDLE_DIR',str(ROOT/'data/gold/gold_20261120'))).expanduser().resolve()
    if not (folder/'bundle.json').exists() and list(Path('output/gold').glob('*/*/summary.json')):
        runpy.run_path(str(ROOT/'examples/pilot_dashboard.py'),run_name='__main__');return
    st.set_page_config(page_title='GPU capacity review',page_icon='◈',layout='wide',initial_sidebar_state='auto')
    st.markdown('''<style>
    .stApp {background:#f6f8fb;color:#18283c;font-family:Inter,system-ui,sans-serif}
    [data-testid="stSidebar"] {background:#fff;border-right:1px solid #e0e6ef}
    .block-container {padding-top:3.8rem;padding-bottom:2rem;max-width:1120px}
    h1,h2,h3 {white-space:nowrap;letter-spacing:-.025em;color:#15263e}
    h1 {font-size:clamp(1.4rem,4vw,2.2rem)!important}
    h2,h3 {font-size:clamp(1.1rem,2.2vw,1.4rem)!important;margin-top:.6rem!important}
    [data-testid="stMetric"] {background:white;border:1px solid #e0e6ef;border-radius:10px;padding:12px 15px}
    [data-testid="stMetricLabel"] p {font-size:.88rem!important;color:#53647a}
    [data-testid="stMetricValue"] {font-size:1.65rem}
    @media(max-width:768px) {
      .block-container {padding-left:1rem;padding-right:1rem}
      div[data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.6rem}
      div[data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) > div {min-width:0;width:100%}
      [data-testid="stMetricValue"] {font-size:1.4rem}
    }
    </style>''',unsafe_allow_html=True)
    st.title('GPU capacity review')
    if not (folder/'bundle.json').exists():
        if 'GOLD_BUNDLE_DIR' in os.environ:
            st.error('GOLD_BUNDLE_DIR does not contain a verified Gold bundle.');return
        try:
            spec_json=(ROOT/'app/gold_release.json').read_text()
            folder=hosted_bundle(spec_json,str(folder))
        except (OSError,ValueError,KeyError) as error:
            st.error('The saved dataset could not be prepared. Reboot the app to retry.')
            st.exception(error);return
    try:m=catalog(str(folder),(folder/'bundle.json').stat().st_mtime_ns)
    except (ValueError,OSError,KeyError) as error:
        st.error(f'Snapshot verification failed: {error}');return
    if m.get('synthetic'):st.caption('Synthetic demo · not AWS trace results')
    f=str(folder);identity=m['plan_identity'];capacity=data(f,'capacity_hourly',identity)
    models=sorted(capacity.gpu_spec_public.dropna().unique().tolist())
    clusters=sorted(capacity.cluster_id.dropna().unique().tolist())
    aliases={raw:f'Cluster {i+1:02d}' for i,raw in enumerate(clusters)}
    reverse={label:raw for raw,label in aliases.items()}
    with st.sidebar:
        st.markdown('### Scope')
        days=st.slider('Trace days',int(min(m['relative_days'])),int(max(m['relative_days'])),
                       (int(min(m['relative_days'])),int(max(m['relative_days']))),key='days')
        cluster_label=st.selectbox('Cluster',['All clusters']+list(reverse),key='cluster')
        exclude=st.checkbox('Exclude flagged requests',value=True,key='exclude',help='Removes source-inconsistent hours and days with unproven duplicate candidates. Inventory remains visible.')
        st.caption('Relative days, not dates. Saved data, not live monitoring.')
    cluster=reverse.get(cluster_label)
    raw=select(capacity,days,None,cluster)
    clean=select(capacity,days,None,cluster,exclude)
    if raw.empty:st.info('No inventory in this window.');return
    all_hours=hourly(raw);used_hours=hourly(clean)
    with st.sidebar:
        st.caption(f'{len(used_hours):,}/{len(all_hours):,} hours used'+(f' · {len(all_hours)-len(used_hours):,} flagged hours excluded' if exclude else ' · flags included'))
    if not exclude and (raw.source_anomaly.any() or raw.duplicate_candidates_present.any()):
        st.warning('Flagged requests may be inflated or inconsistent.')

    summary=cohort_summary(clean,1.0)
    ranked_summary=summary.loc[summary['p95 request / inventory'].notna()] if not summary.empty else summary
    order=ranked_summary['GPU cohort'].tolist() if not ranked_summary.empty else []
    order += [v for v in sorted(raw.gpu_spec_public.dropna().unique().tolist()) if v not in order]
    if st.session_state.get('focus') not in order:st.session_state['focus']=order[0]
    focus=st.session_state['focus']
    st.caption(f'{focus} · Days {days[0]}–{days[1]} · {cluster_label}')
    pressure_tab,review_tab,growth_tab=st.tabs(['1 · Demand pressure','2 · Existing GPUs','3 · Plan for growth'])
    scoped=select(capacity,days,focus,cluster)
    screened=select(capacity,days,focus,cluster,exclude)
    h=hourly(screened);inv=hourly(scoped)
    with pressure_tab:
        st.header('Where is demand highest?')
        if not ranked_summary.empty:
            if focus in ranked_summary['GPU cohort'].values:
                rank=ranked_summary['GPU cohort'].tolist().index(focus)+1
                ratio=float(ranked_summary.loc[ranked_summary['GPU cohort'].eq(focus),'p95 request / inventory'].iloc[0])
                finding=f'{focus} has the highest request pressure at {ratio:.2f}× inventory.' if rank==1 else f'{focus} ranks {rank} of {len(ranked_summary)} at {ratio:.2f}× inventory.'
                st.write(f'**{finding}**')
            else:st.write(f'**No screened requests for {focus}.**')
            ranked=ranked_summary[['GPU cohort','p95 request / inventory','Hours with request evidence']].copy()
            ranked['Selected']=ranked['GPU cohort'].eq(focus)
            pick=alt.selection_point(name='gpu',fields=['GPU cohort'],on='click',toggle=False,clear=False)
            bars=alt.Chart(ranked).mark_bar(cornerRadiusEnd=3,cursor='pointer').add_params(pick).encode(
                y=alt.Y('GPU cohort:N',sort=alt.SortField(field='p95 request / inventory',order='descending'),title=None,axis=alt.Axis(labelOverlap=False,labelFontSize=14)),
                x=alt.X('p95 request / inventory:Q',title='High-hour requests ÷ available GPUs',axis=alt.Axis(format='.1f')),
                color=alt.condition(alt.datum.Selected,alt.value('#118a7e'),alt.value('#ccd5e3')),
                tooltip=['GPU cohort',alt.Tooltip('p95 request / inventory:Q',title='Request ratio',format='.2f'),'Hours with request evidence'])
            st.altair_chart(bars.properties(height=max(160,len(ranked)*24)).configure_view(stroke=None),use_container_width=True,key='pressure_chart',on_select=choose_gpu,selection_mode=['gpu'])
            st.caption('Click a bar to select across all tabs. Requests do not prove a shortage. High-hour pressure is the 95th percentile: 95% of measured hours are at or below it. 1× means requests equal inventory.')
        else:st.info('No screened request evidence. Inventory remains available.')
        missing=[v for v in order if v not in ranked_summary.get('GPU cohort',pd.Series(dtype=str)).tolist()]
        if missing:
            with st.expander('GPU types without request measurements'):
                for model in missing:
                    if st.button(model,key='choose_'+model):
                        st.session_state['focus']=model;st.rerun()
        selected_rows=summary.loc[summary['GPU cohort'].eq(focus)] if not summary.empty else pd.DataFrame()
        selected=selected_rows.iloc[0] if not selected_rows.empty else None
        c=st.columns(3)
        c[0].metric(f'{focus} · available GPUs',number(inv.inventory_gpu_slots.iloc[-1]),help='Slots at the latest recorded inventory hour.')
        c[1].metric('High-hour request pressure',f'{selected["p95 request / inventory"]:.2f}×' if selected is not None and pd.notna(selected['p95 request / inventory']) else 'Unknown',help='p95 of observed hourly request/inventory ratios; same-hour observations need not run simultaneously.')
        c[2].metric('Hours ≥ inventory',percent(selected['Hours above review threshold'] if selected is not None else np.nan),help='Share of evidenced hours with requests at or above inventory.')
        with st.expander('Hourly evidence'):
            timeline=inv[['day','hour','inventory_gpu_slots']].merge(h[['day','hour','compatible_observed_gpu_requests']] if not h.empty else pd.DataFrame(columns=['day','hour','compatible_observed_gpu_requests']),on=['day','hour'],how='left')
            timeline['trace_hour']=timeline.day*24+timeline.hour;timeline['trace_day']=timeline.day+timeline.hour/24
            line_chart(timeline,{'inventory_gpu_slots':'Inventory','compatible_observed_gpu_requests':'Observed requests'},'GPU slots')
            st.caption('Excluded periods remain gaps. Capacity is counted once per server-hour.')

    with review_tab:
        st.header('Could existing GPUs work harder?')
        st.caption(f'{focus} · Request placement and processing activity are separate evidence.')
        a=eligible_activity(data(f,'activity_evidence_hourly',identity),capacity,days,focus,cluster,exclude)
        st.subheader('How busy were the processors?')
        if a.empty:
            st.info('No activity measurements for this selection.')
        else:
            measured=safe_divide(a.sm_valid_observations.sum(),a.gpu_requesting_observations.sum())
            c=st.columns(2)
            c[0].metric('Average recorded activity',number(safe_divide(a.sm_source_unit_sum.sum(),a.sm_valid_observations.sum()),1))
            c[1].metric('Missing activity measurements',percent(1-measured))
            activity=a.groupby(['day','hour'],observed=True)[['sm_source_unit_sum','sm_valid_observations']].sum().reset_index()
            activity['activity']=activity.sm_source_unit_sum/activity.sm_valid_observations.replace(0,np.nan)
            activity['trace_hour']=activity.day*24+activity.hour;activity['trace_day']=activity.day+activity.hour/24
            line_chart(activity,{'activity':'Recorded GPU activity'},'Recorded GPU activity')
            st.caption('Source scale; not a verified percentage.')
            st.caption('Missing readings stay unknown; averages use measured observations.')
        st.subheader('Where were fewer GPUs requested?')
        placement=select(data(f,'placement_daily',identity),days,focus,cluster)
        bins=placement.groupby('headroom_bin',observed=True)['count'].sum().reset_index()
        if not bins.empty and bins['count'].sum()>0:
            unknown=safe_divide(bins.loc[bins.headroom_bin.eq('unknown'),'count'].sum(),bins['count'].sum())
            labels={'negative':'Over-requested','zero':'No headroom','(0,1]':'Up to 1 slot','(1,4]':'1–4 slots','>4':'>4 slots','unknown':'Unknown'}
            bins['Bin']=bins.headroom_bin.astype('object').map(labels)
            bins['Share']=bins['count']/bins['count'].sum()
            bins['Color']=bins.headroom_bin.astype('object').map(lambda v:'#c5ccd7' if v=='unknown' else '#da6b59' if v=='negative' else '#118a7e')
            st.altair_chart(alt.Chart(bins).mark_bar(size=20,cornerRadiusEnd=2).encode(
                y=alt.Y('Bin:N',sort=list(labels.values()),title=None,axis=alt.Axis(labelFontSize=13,labelOverlap=False)),
                x=alt.X('Share:Q',title='Share of server observations',axis=alt.Axis(format='%')),
                color=alt.Color('Color:N',scale=None,legend=None),
                tooltip=['Bin:N',alt.Tooltip('Share:Q',format='.1%'),alt.Tooltip('count:Q',title='Server-hours',format=',')]
            ).properties(height=190).configure_view(stroke=None),use_container_width=True)
            st.caption(f'{percent(unknown)} unknown. Each observation is one server in one hour. Fewer requests do not confirm spare GPUs.')
        else:st.caption('No placement evidence for this selection.')

    with growth_tab:
        st.header('What would growth require?')
        st.caption(f'Illustrative sizing for {focus}. Requests are assumed to represent demand; this is not a purchase recommendation.')
        x,y,z=st.columns(3)
        growth=x.slider('Request growth',0,200,20,5,format='%d%%')/100
        reserve=y.slider('Spare capacity',0,80,20,5,format='%d%%')/100
        anchor=z.radio('Demand reference',['p95','peak'],format_func=lambda v:'High demand (95th percentile)' if v=='p95' else 'Highest observed',horizontal=True)
        baseline=st.radio('Available GPUs',['Latest observed hour','Minimum observed capacity in window'],horizontal=True)
        st.caption('Demand estimates always exclude flagged records. Spare capacity is the fraction of modeled capacity kept in reserve.')
        result=scenario(scoped,growth,reserve,anchor,baseline)
        if result is None:st.info('Insufficient screened requests for a scenario.')
        else:
            c=st.columns(2)
            c[0].metric('Observed request anchor',number(result['anchor'],1))
            c[1].metric('Inventory reference',number(result['capacity']))
            if st.checkbox('Model requests as demand',value=False,key='scenario_ack'):
                c=st.columns(2)
                c[0].metric('Modeled required slots',number(result['modeled_slots']))
                c[1].metric('Modeled additional slots',number(result['additional_slots']))
                st.caption('ceil(anchor × (1 + growth) / (1 − reserve)). Validate running demand, compatibility and service targets before buying.')
            else:st.caption('Enable the demand assumption to see the modeled gap.')

    with st.expander('Data and definitions'):
        detail=st.radio('Detail',['Quality','Workload mix','Network','Execution','AWS evidence'],key='detail')
        if detail=='Quality':
            st.write('Flags exclude unproven duplicate-candidate days and the day-5 hour-06 inconsistency. Missing observations stay unknown. All procurement-readiness flags remain false.')
            q=select(data(f,'planning_quality',identity),days,focus,cluster)
            st.dataframe(q.groupby(['inventory_match','cohort_match'],observed=True)[['observations','valid_request_observations']].sum().reset_index(),hide_index=True,use_container_width=True)
            st.caption('Server-hour coverage measures record presence, not complete demand. SM means use valid observation counts; values above 100 remain in source units.')
        elif detail=='Workload mix':
            dimension=st.selectbox('Group by',['job_type_public','priority_class','model_type_public','state_public','is_genai_request'])
            mix=mix_data(f,identity,dimension,days[0],days[1],focus,cluster)
            if exclude:mix=mix.merge(screened[['day','hour','gpu_spec_public','cluster_id']],on=['day','hour','gpu_spec_public','cluster_id'],how='inner',validate='many_to_one')
            mix=mix.loc[mix.inventory_match & mix.cohort_match].copy()
            mix['Category']=mix['value'].astype('object').where(mix['value'].notna(),'(missing)')
            grouped=mix.groupby('Category',observed=True)[['observations','observed_gpu_requests']].sum().reset_index()
            st.dataframe(grouped,hide_index=True,use_container_width=True)
            csv_download(grouped,'Download mix','workload-mix.csv')
        elif detail=='Network':
            n=select(data(f,'network_context_hourly',identity),days,focus,cluster)
            st.caption('Days 109–115 only. GiB/s per observed server; link saturation is unknown.')
            if n.empty:st.info('No aligned network data in this window.')
            else:
                n=n.groupby(['day','hour'],observed=True)[['rx_gibps_sum','rx_valid_observations','tx_gibps_sum','tx_valid_observations']].sum().reset_index()
                n['rx']=n.rx_gibps_sum/n.rx_valid_observations.replace(0,np.nan);n['tx']=n.tx_gibps_sum/n.tx_valid_observations.replace(0,np.nan)
                n['trace_hour']=n.day*24+n.hour;n['trace_day']=n.day+n.hour/24
                line_chart(n,{'rx':'Receive','tx':'Transmit'},'GiB/s')
        elif detail=='Execution':
            st.caption('Global context. Cohort filter applies; day and cluster do not. Observed spans are not guaranteed completed runtimes.')
            ex=data(f,'execution_context_global',identity);ex=ex.loc[ex.gpu_spec_public.eq(focus)]
            ex=ex.groupby('job_type_public',observed=True)[['observed_execution_records','observed_span_hours_sum','valid_span_records']].sum().reset_index()
            ex['Mean span · hours']=ex.observed_span_hours_sum/ex.valid_span_records.replace(0,np.nan)
            st.dataframe(ex[['job_type_public','observed_execution_records','Mean span · hours']],hide_index=True,use_container_width=True)
        else:
            if m.get('synthetic'):st.info('Synthetic UI fixture, not AWS run evidence.')
            st.write('Airflow + Spark on EC2 → S3 Bronze, Silver and Gold → verified local snapshot.')
            st.caption('6.76 billion Silver rows · 185 Gold days · 8h52m Gold runtime. The dashboard makes no AWS requests.')
            st.link_button('AWS run evidence','https://github.com/shrutidoshi94/gpu-fleet-analytics/tree/gpu-fleet-analytics')
            st.dataframe(pd.DataFrame([{'Product':k,'Rows':v['rows']} for k,v in m['tables'].items()]),hide_index=True,use_container_width=True)
            st.code(f'Release: {m["release"]}\nPlan: {m["plan_identity"]}\nCode: {m["code_sha256"]}')
            st.dataframe(pd.DataFrame([{'Alias':v,'Cluster ID':k} for k,v in aliases.items()]),hide_index=True,use_container_width=True)
        csv_download(summary,'Download cohort comparison','cohort-comparison.csv')


if __name__=='__main__':
    alt.data_transformers.disable_max_rows()
    main()
