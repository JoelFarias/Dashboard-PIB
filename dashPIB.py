import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import plotly.io as pio
import gc

# Configuração da página
st.set_page_config(
    layout="wide", 
    page_title="PIB Municípios Brasil",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# Tema personalizado para Plotly
pio.templates["custom_theme"] = go.layout.Template(
    layout=dict(
        font=dict(family="Arial, sans-serif", color="#333"),
        title=dict(font=dict(family="Arial, sans-serif", size=24, color="#1E3A8A"), x=0.5),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        colorway=px.colors.qualitative.Plotly,
        legend=dict(
            bgcolor="#F9FAFB",
            bordercolor="#E5E7EB",
            borderwidth=1,
            font=dict(family="Arial, sans-serif", size=12, color="#333")
        ),
        xaxis=dict(
            gridcolor="#E5E7EB",
            zerolinecolor="#D1D5DB",
            title=dict(font=dict(family="Arial, sans-serif", size=14, color="#1E3A8A")),
            tickfont=dict(color="#4B5563")
        ),
        yaxis=dict(
            gridcolor="#E5E7EB",
            zerolinecolor="#D1D5DB",
            title=dict(font=dict(family="Arial, sans-serif", size=14, color="#1E3A8A")),
            tickfont=dict(color="#4B5563")
        ),
        hoverlabel=dict(
            bgcolor="#1E3A8A",
            font_size=12,
            font_family="Arial, sans-serif",
            font_color="white"
        ),
        transition=dict(duration=500, easing="cubic-in-out")
    )
)

pio.templates.default = "plotly_white+custom_theme"

# CSS personalizado
st.markdown("""
<style>
    body {
        font-family: 'Arial', sans-serif;
    }
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
        padding-bottom: 1rem;
        border-bottom: 2px solid #E5E7EB;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #1E3A8A;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #DBEAFE;
        opacity: 0;
        transform: translateY(20px);
        animation: slideUpFadeIn 0.6s ease-out forwards;
    }
    @keyframes slideUpFadeIn {
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    .card {
        background-color: #F9FAFB;
        border-radius: 0.5rem;
        padding: 1.2rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
        margin-bottom: 1rem;
        transition: all 0.3s cubic-bezier(.25,.8,.25,1);
    }
    .card:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.16), 0 3px 6px rgba(0,0,0,0.23);
        transform: translateY(-2px);
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1E3A8A;
    }
    .metric-label {
        font-size: 1rem;
        color: #6B7280;
    }
    .footer {
        text-align: center;
        margin-top: 3rem;
        padding-top: 1rem;
        border-top: 1px solid #E5E7EB;
        color: #6B7280;
        font-size: 0.8rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #2563EB;
        color: white;
        border: none;
        padding: 10px;
        border-radius: 0.375rem;
        transition: background-color 0.2s ease-in-out;
    }
    .stButton>button:hover {
        background-color: #1D4ED8;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        border-bottom: 2px solid #DBEAFE;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #F3F4F6;
        border-radius: 4px 4px 0 0;
        gap: 1px;
        padding: 10px 15px;
        transition: background-color 0.2s ease-in-out, color 0.2s ease-in-out;
    }
    .stTabs [aria-selected="true"] {
        background-color: #DBEAFE;
        color: #1E3A8A;
        font-weight: bold;
        border-bottom: 2px solid #1E3A8A;
    }
    @keyframes fadeIn {
        0% { opacity: 0; }
        100% { opacity: 1; }
    }
    .animate-fade-in {
        animation: fadeIn 0.5s ease-in-out;
    }
    .dataframe-container {
        margin-top: 1rem;
    }
    .stDataFrame {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource(max_entries=1)
def obter_conexao():
    try:
        db_credentials = st.secrets["postgres"]
        connection_string = f"postgresql://{db_credentials['user']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['database']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

@st.cache_data(max_entries=100, persist="disk")
def carregar_dados_db(query, params=None):
    engine = obter_conexao()
    if engine is None:
        st.error("Não foi possível estabelecer conexão com o banco de dados.")
        st.stop()
    
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

@st.cache_data(max_entries=1, persist="disk")
def obter_anos_disponiveis():
    query = "SELECT DISTINCT ano_pib FROM pib_municipios ORDER BY ano_pib"
    df = carregar_dados_db(query)
    return df['ano_pib'].tolist()

@st.cache_data(max_entries=1, persist="disk")
def obter_ufs_disponiveis():
    query = "SELECT cd_uf, sigla_uf, nome_uf FROM unidade_federacao ORDER BY sigla_uf"
    return carregar_dados_db(query)

@st.cache_data(max_entries=30, persist="disk")
def obter_municipios_por_ufs(ufs):
    if not ufs:
        return pd.DataFrame(columns=['codigo_municipio_dv', 'nome_municipio', 'cd_uf', 'municipio_capital'])
    
    placeholders = ', '.join(['%s'] * len(ufs))
    query = f"""
    SELECT codigo_municipio_dv, nome_municipio, cd_uf, municipio_capital 
    FROM municipio 
    WHERE cd_uf IN ({placeholders}) 
    ORDER BY nome_municipio
    """
    return carregar_dados_db(query, params=tuple(ufs))

@st.cache_data(max_entries=50, persist="disk")
def obter_dados_filtrados(anos, ufs, municipios_codigos):
    if not ufs or not municipios_codigos:
        return pd.DataFrame()
    
    ano_inicial, ano_final = anos
    placeholders_municipios = ', '.join(['%s'] * len(municipios_codigos))
    
    query = f"""
    SELECT 
        p.ano_pib, p.codigo_municipio_dv, 
        p.vl_agropecuaria, p.vl_industria, p.vl_servicos, 
        p.vl_administracao, p.vl_bruto_total, p.vl_subsidios, 
        p.vl_pib, p.vl_pib_per_capta,
        m.nome_municipio, m.cd_uf, m.municipio_capital, m.longitude, m.latitude,
        u.sigla_uf, u.nome_uf, u.cd_regiao
    FROM pib_municipios p
    JOIN municipio m ON p.codigo_municipio_dv = m.codigo_municipio_dv
    JOIN unidade_federacao u ON m.cd_uf = u.cd_uf
    WHERE p.ano_pib BETWEEN %s AND %s
    AND p.codigo_municipio_dv IN ({placeholders_municipios})
    """
    
    params = [ano_inicial, ano_final] + municipios_codigos
    df = carregar_dados_db(query, params=tuple(params))
    
    # Adicionar coluna populacao_estimada se não existir
    if 'populacao_estimada' not in df.columns and not df.empty:
         df['populacao_estimada'] = df.apply(
             lambda row: int(row['vl_pib'] / row['vl_pib_per_capta']) 
             if row['vl_pib_per_capta'] > 0 else 0, 
             axis=1
         )
    
    gc.collect()
    return df

def criar_cards_kpi(df_ultimo_ano, df_primeiro_ano, primeiro_ano, ultimo_ano):
    if df_ultimo_ano.empty:
        st.info("Não há dados para o último ano selecionado para exibir KPIs.")
        return
    
    pib_total_ultimo = df_ultimo_ano['vl_pib'].sum()
    
    if 'populacao_estimada' in df_ultimo_ano.columns and df_ultimo_ano['populacao_estimada'].sum() > 0:
        pib_per_capita_ultimo = df_ultimo_ano['vl_pib'].sum() / df_ultimo_ano['populacao_estimada'].sum()
    else:
        pib_per_capita_ultimo = df_ultimo_ano['vl_pib_per_capta'].mean()

    variacao_texto, variacao_cor = "N/A", "gray"
    if primeiro_ano != ultimo_ano and not df_primeiro_ano.empty:
        pib_total_primeiro = df_primeiro_ano['vl_pib'].sum()
        if pib_total_primeiro > 0:
            variacao_pib = ((pib_total_ultimo - pib_total_primeiro) / pib_total_primeiro) * 100
            variacao_texto = f"{variacao_pib:.2f}%"
            variacao_cor = "green" if variacao_pib >= 0 else "red"
    
    total_agro = df_ultimo_ano['vl_agropecuaria'].sum()
    total_industria = df_ultimo_ano['vl_industria'].sum()
    total_servicos = df_ultimo_ano['vl_servicos'].sum()
    total_adm = df_ultimo_ano['vl_administracao'].sum()
    setores_valores = {
        "Serviços": total_servicos, 
        "Indústria": total_industria, 
        "Agropecuária": total_agro, 
        "Administração": total_adm
    }
    maior_setor = max(setores_valores, key=setores_valores.get) if setores_valores else "N/A"
    maior_valor_setor = setores_valores.get(maior_setor, 0)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class='card animate-fade-in'>
            <p class='metric-label'>PIB Total ({ultimo_ano})</p>
            <p class='metric-value'>R$ {pib_total_ultimo/1e9:.2f} bi</p>
            <p style='color: {variacao_cor};'>{variacao_texto} desde {primeiro_ano}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class='card animate-fade-in'>
            <p class='metric-label'>PIB Per Capita ({ultimo_ano})</p>
            <p class='metric-value'>R$ {pib_per_capita_ultimo:,.2f}</p>
            <p>Média dos municípios</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class='card animate-fade-in'>
            <p class='metric-label'>Municípios Analisados</p>
            <p class='metric-value'>{df_ultimo_ano['nome_municipio'].nunique()}</p>
            <p>De {df_ultimo_ano['cd_uf'].nunique()} estados</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class='card animate-fade-in'>
            <p class='metric-label'>Maior Setor Econômico</p>
            <p class='metric-value'>{maior_setor}</p>
            <p>R$ {maior_valor_setor/1e9:.2f} bi</p>
        </div>
        """, unsafe_allow_html=True)

def criar_graficos_evolucao(df_filtrado, tipo_visualizacao):
    if df_filtrado.empty:
        st.info("Sem dados para exibir gráficos de evolução.")
        return
    
    if tipo_visualizacao == "PIB Total":
        y_col, y_label, titulo = 'vl_pib', 'PIB Total (R$)', 'Evolução do PIB Total'
        df_evolucao = df_filtrado.groupby('ano_pib', as_index=False)[y_col].sum()
    else:
        y_col, y_label, titulo = 'vl_pib_per_capta', 'PIB Per Capita (R$)', 'Evolução do PIB Per Capita'
        df_evolucao = df_filtrado.groupby('ano_pib', as_index=False)[y_col].mean()

    fig = px.area(
        df_evolucao, 
        x='ano_pib', 
        y=y_col, 
        title=titulo, 
        labels={'ano_pib': 'Ano', y_col: y_label}, 
        markers=True
    )
    
    fig.update_layout(
        xaxis_tickmode='linear', 
        hovermode='x unified', 
        height=400, 
        margin=dict(t=50, b=40)
    )
    
    st.plotly_chart(fig, use_container_width=True)

    if len(df_evolucao) > 1:
        df_evolucao['taxa_crescimento'] = df_evolucao[y_col].pct_change() * 100
        fig_taxa = px.bar(
            df_evolucao.dropna(), 
            x='ano_pib', 
            y='taxa_crescimento', 
            title='Taxa de Crescimento Anual (%)', 
            labels={'taxa_crescimento': 'Crescimento (%)'}
        )
        
        fig_taxa.update_layout(
            height=300, 
            margin=dict(t=50, b=40)
        )
        
        st.plotly_chart(fig_taxa, use_container_width=True)

def criar_graficos_ranking(df_ultimo_ano, tipo_visualizacao, ultimo_ano, num_ranking, mostrar_capitais):
    if df_ultimo_ano.empty:
        st.info("Sem dados para exibir gráficos de ranking.")
        return
    
    if tipo_visualizacao == "PIB Total":
        valor_col = 'vl_pib'
        titulo_ranking = f'Top {num_ranking} Municípios por PIB Total em {ultimo_ano}'
        label_valor = 'PIB Total (R$)'
    else:
        valor_col = 'vl_pib_per_capta'
        titulo_ranking = f'Top {num_ranking} Municípios por PIB Per Capita em {ultimo_ano}'
        label_valor = 'PIB Per Capita (R$)'
    
    df_ranking = df_ultimo_ano.sort_values(valor_col, ascending=False).head(num_ranking)
    df_ranking['nome_exibicao'] = df_ranking.apply(
        lambda x: f"{x['nome_municipio']} {'🏛️' if x['municipio_capital'] and mostrar_capitais else ''}", 
        axis=1
    )
    
    fig_ranking = px.bar(
        df_ranking, 
        x=valor_col, 
        y='nome_exibicao', 
        orientation='h', 
        title=titulo_ranking, 
        labels={valor_col: label_valor, 'nome_exibicao': 'Município'}, 
        color='sigla_uf', 
        text=valor_col
    )
    
    fig_ranking.update_traces(
        texttemplate='R$ %{text:,.2s}' if tipo_visualizacao == "PIB Total" else 'R$ %{text:,.2f}', 
        textposition='outside'
    )
    
    fig_ranking.update_layout(
        yaxis_categoryorder='total ascending', 
        height=max(400, num_ranking * 35), 
        margin=dict(l=150)
    )
    
    st.plotly_chart(fig_ranking, use_container_width=True)

    if st.checkbox("Mostrar municípios com menor PIB", key=f"bottom_ranking_{tipo_visualizacao}"):
        df_ranking_inv = df_ultimo_ano.sort_values(valor_col).head(num_ranking)
        df_ranking_inv['nome_exibicao'] = df_ranking_inv.apply(
            lambda x: f"{x['nome_municipio']} {'🏛️' if x['municipio_capital'] and mostrar_capitais else ''}", 
            axis=1
        )
        
        titulo_inv = f'Bottom {num_ranking} Municípios por {tipo_visualizacao} em {ultimo_ano}'
        
        fig_ranking_inv = px.bar(
            df_ranking_inv, 
            x=valor_col, 
            y='nome_exibicao', 
            orientation='h', 
            title=titulo_inv, 
            labels={valor_col: label_valor, 'nome_exibicao': 'Município'}, 
            color='sigla_uf', 
            text=valor_col
        )
        
        fig_ranking_inv.update_traces(
            texttemplate='R$ %{text:,.2s}' if tipo_visualizacao == "PIB Total" else 'R$ %{text:,.2f}', 
            textposition='outside'
        )
        
        fig_ranking_inv.update_layout(
            yaxis_categoryorder='total descending', 
            height=max(400, num_ranking * 35), 
            margin=dict(l=150)
        )
        
        st.plotly_chart(fig_ranking_inv, use_container_width=True)

def criar_graficos_setoriais(df_ultimo_ano, ultimo_ano):
    if df_ultimo_ano.empty:
        st.info("Sem dados para exibir gráficos setoriais.")
        return
    
    df_setorial_uf = df_ultimo_ano.groupby('sigla_uf', as_index=False).agg(
        vl_agropecuaria=('vl_agropecuaria', 'sum'), 
        vl_industria=('vl_industria', 'sum'), 
        vl_servicos=('vl_servicos', 'sum'), 
        vl_administracao=('vl_administracao', 'sum')
    )
    
    setores = ['vl_agropecuaria', 'vl_industria', 'vl_servicos', 'vl_administracao']
    setores_nomes = ['Agropecuária', 'Indústria', 'Serviços', 'Administração Pública']
    
    fig_setorial = go.Figure()
    
    for setor, nome_setor in zip(setores, setores_nomes):
        fig_setorial.add_trace(go.Bar(
            x=df_setorial_uf['sigla_uf'], 
            y=df_setorial_uf[setor], 
            name=nome_setor, 
            hovertemplate='%{y:,.2f}'
        ))
    
    fig_setorial.update_layout(
        title=f'Composição Setorial do PIB por UF ({ultimo_ano})', 
        xaxis_title='UF', 
        yaxis_title='Valor (R$)', 
        barmode='stack', 
        height=500, 
        legend_title_text='Setores'
    )
    
    st.plotly_chart(fig_setorial, use_container_width=True)

    total_setorial = {nome: df_ultimo_ano[col].sum() for nome, col in zip(setores_nomes, setores)}
    df_pizza = pd.DataFrame(list(total_setorial.items()), columns=['Setor', 'Valor'])
    
    fig_pizza = px.pie(
        df_pizza, 
        values='Valor', 
        names='Setor', 
        title=f'Distribuição Setorial Agregada ({ultimo_ano})', 
        hole=0.4
    )
    
    fig_pizza.update_traces(
        textposition='inside', 
        textinfo='percent+label'
    )
    
    fig_pizza.update_layout(
        height=450, 
        legend_x=1.1
    )
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.plotly_chart(fig_pizza, use_container_width=True)
    
    with col2:
        st.markdown("#### Destaques Setoriais")
        for setor, valor in total_setorial.items():
            st.markdown(f"- **{setor}:** R$ {valor/1e9:.2f} bi")

def criar_graficos_geograficos(df_ultimo_ano, tipo_visualizacao, ultimo_ano):
    if df_ultimo_ano.empty or 'latitude' not in df_ultimo_ano.columns or 'longitude' not in df_ultimo_ano.columns:
        st.info("Dados geográficos insuficientes ou ausentes para exibir mapa.")
        return
    
    if tipo_visualizacao == "PIB Total":
        valor_col = 'vl_pib'
        titulo_geo = f'Distribuição Geográfica do PIB Total ({ultimo_ano})'
        label_valor = 'PIB Total (R$)'
    else:
        valor_col = 'vl_pib_per_capta'
        titulo_geo = f'Distribuição Geográfica do PIB Per Capita ({ultimo_ano})'
        label_valor = 'PIB Per Capita (R$)'
    
    df_geo_valid = df_ultimo_ano.dropna(subset=['latitude', 'longitude', valor_col])
    
    if df_geo_valid.empty:
        st.info("Não há dados válidos para o mapa após remover valores ausentes.")
        return

    fig_geo = px.scatter_mapbox(
        df_geo_valid, 
        lat='latitude', 
        lon='longitude', 
        color=valor_col, 
        size=valor_col,
        hover_name='nome_municipio', 
        size_max=30, 
        zoom=3, 
        height=600,
        title=titulo_geo, 
        labels={valor_col: label_valor},
        color_continuous_scale=px.colors.cyclical.IceFire,
        hover_data={
            'sigla_uf': True, 
            valor_col: ':.2f', 
            'vl_pib_per_capta' if tipo_visualizacao == "PIB Total" else 'vl_pib': ':.2f'
        }
    )
    
    fig_geo.update_layout(
        mapbox_style="open-street-map", 
        margin={"r":0,"t":40,"l":0,"b":0}
    )
    
    st.plotly_chart(fig_geo, use_container_width=True)

    df_regiao = df_ultimo_ano.groupby(['cd_regiao', 'nome_uf'], as_index=False).agg(
        valor_agregado=(valor_col, 'sum'), 
        num_municipios=('nome_municipio', 'count')
    )
    
    fig_regiao = px.treemap(
        df_regiao, 
        path=['cd_regiao', 'nome_uf'], 
        values='valor_agregado', 
        color='valor_agregado', 
        title=f'Distribuição do {tipo_visualizacao} por Região e UF ({ultimo_ano})', 
        hover_data=['num_municipios']
    )
    
    fig_regiao.update_layout(height=500)
    st.plotly_chart(fig_regiao, use_container_width=True)

def criar_tabela_dados(df_filtrado, mostrar_tabela):
    if mostrar_tabela and not df_filtrado.empty:
        st.markdown("<h2 class='sub-header'>Dados Filtrados</h2>", unsafe_allow_html=True)
        
        colunas_exibir = [
            'ano_pib', 'nome_municipio', 'sigla_uf', 'vl_pib', 
            'vl_pib_per_capta', 'vl_agropecuaria', 'vl_industria', 
            'vl_servicos', 'vl_administracao'
        ]
        
        df_tabela = df_filtrado[[col for col in colunas_exibir if col in df_filtrado.columns]].copy()
        df_tabela.columns = [
            'Ano', 'Município', 'UF', 'PIB Total (R$)', 
            'PIB Per Capita (R$)', 'Agropecuária (R$)', 
            'Indústria (R$)', 'Serviços (R$)', 'Adm. Pública (R$)'
        ]
        
        st.dataframe(
            df_tabela.style.format({
                'PIB Total (R$)': "R$ {:,.2f}", 
                'PIB Per Capita (R$)': "R$ {:,.2f}",
                'Agropecuária (R$)': "R$ {:,.2f}", 
                'Indústria (R$)': "R$ {:,.2f}",
                'Serviços (R$)': "R$ {:,.2f}", 
                'Adm. Pública (R$)': "R$ {:,.2f}"
            }), 
            use_container_width=True
        )
        
        csv = df_tabela.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download (CSV)", 
            data=csv, 
            file_name=f"pib_municipios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
            mime="text/csv"
        )
    elif mostrar_tabela:
        st.info("Nenhum dado para exibir na tabela com os filtros atuais.")

def main():
    st.markdown("<h1 class='main-header'>📊 Dashboard do PIB dos Municípios Brasileiros</h1>", unsafe_allow_html=True)

    with st.spinner("Carregando dados iniciais..."):
        anos_disponiveis = obter_anos_disponiveis()
        ufs_df = obter_ufs_disponiveis()

    st.sidebar.markdown("<h2 style='text-align: center; color: #1E3A8A;'>Filtros</h2><hr>", unsafe_allow_html=True)
    
    anos_selecionados = st.sidebar.slider(
        "Período (Ano)", 
        min(anos_disponiveis), 
        max(anos_disponiveis), 
        (min(anos_disponiveis), max(anos_disponiveis)), 
        help="Selecione o intervalo de anos."
    )
    
    ufs_selecionadas = st.sidebar.multiselect(
        "Estado(s) (UF)", 
        ufs_df['sigla_uf'].tolist(), 
        default=ufs_df['sigla_uf'].tolist()[:min(5, len(ufs_df))], 
        format_func=lambda x: f"{x} - {ufs_df[ufs_df['sigla_uf'] == x]['nome_uf'].iloc[0]}", 
        help="Selecione um ou mais estados."
    )
    
    municipios_df = obter_municipios_por_ufs(ufs_selecionadas)
    
    if not ufs_selecionadas:
        st.sidebar.warning("Selecione ao menos uma UF.")
        municipios_selecionados_nomes = []
    else:
        municipios_disponiveis_nomes = sorted(municipios_df['nome_municipio'].unique().tolist())
        municipios_selecionados_nomes = st.sidebar.multiselect(
            "Município(s)", 
            municipios_disponiveis_nomes, 
            default=municipios_disponiveis_nomes[:min(5, len(municipios_disponiveis_nomes))], 
            help="Selecione um ou mais municípios."
        )

    tipo_visualizacao = st.sidebar.radio(
        "Visualizar por", 
        ["PIB Total", "PIB Per Capita"], 
        help="Escolha a métrica principal."
    )
    
    with st.sidebar.expander("Opções Avançadas"):
        mostrar_capitais = st.checkbox(
            "Destacar Capitais", 
            True, 
            help="Marcar capitais nos gráficos."
        )
        
        mostrar_tabela = st.checkbox(
            "Mostrar Tabela de Dados", 
            True, 
            help="Exibir tabela com dados filtrados."
        )
        
        num_ranking = st.slider(
            "Nº Municípios no Ranking", 
            5, 20, 10, 
            help="Quantidade no ranking."
        )

    if not municipios_selecionados_nomes:
        st.warning("Por favor, selecione ao menos um município para continuar.")
        return
    
    codigos_municipios_selecionados = municipios_df[municipios_df['nome_municipio'].isin(municipios_selecionados_nomes)]['codigo_municipio_dv'].tolist()

    with st.spinner(f"Carregando dados para {len(municipios_selecionados_nomes)} município(s)..."):
        df_filtrado = obter_dados_filtrados(anos_selecionados, ufs_selecionadas, codigos_municipios_selecionados)

    if df_filtrado.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados. Tente ajustar os filtros.")
        return

    st.markdown(f"""
    <div class='animate-fade-in' style='background-color: #F3F4F6; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
        <strong>Filtros:</strong> {anos_selecionados[0]}-{anos_selecionados[1]} | 
        UFs: {', '.join(ufs_selecionadas)} | 
        Municípios: {len(municipios_selecionados_nomes)} | 
        Visualização: {tipo_visualizacao}
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<h2 class='sub-header'>Indicadores Chave</h2>", unsafe_allow_html=True)
    df_ultimo_ano = df_filtrado[df_filtrado['ano_pib'] == anos_selecionados[1]]
    df_primeiro_ano = df_filtrado[df_filtrado['ano_pib'] == anos_selecionados[0]]
    criar_cards_kpi(df_ultimo_ano, df_primeiro_ano, anos_selecionados[0], anos_selecionados[1])

    st.markdown("<h2 class='sub-header'>Visualizações Interativas</h2>", unsafe_allow_html=True)
    tab_titles = ["Evolução Temporal 📈", "Ranking de Municípios 🏆", "Composição Setorial 📊", "Análise Geográfica 🗺️"]
    tab1, tab2, tab3, tab4 = st.tabs(tab_titles)
    
    with tab1:
        criar_graficos_evolucao(df_filtrado, tipo_visualizacao)
    
    with tab2:
        criar_graficos_ranking(df_ultimo_ano, tipo_visualizacao, anos_selecionados[1], num_ranking, mostrar_capitais)
    
    with tab3:
        criar_graficos_setoriais(df_ultimo_ano, anos_selecionados[1])
    
    with tab4:
        criar_graficos_geograficos(df_ultimo_ano, tipo_visualizacao, anos_selecionados[1])
    
    criar_tabela_dados(df_filtrado, mostrar_tabela)

    st.markdown(f"<div class='footer'>Dashboard PIB Municípios | {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)
    st.caption("Fonte dos dados: IBGE")
    gc.collect()

if __name__ == "__main__":
    main()
