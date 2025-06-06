import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime
import gc

def configurar_pagina():
    st.set_page_config(
        layout="wide",
        page_title="PIB dos Municípios do Brasil",
        page_icon="📊",
        initial_sidebar_state="expanded"
    )

def aplicar_estilos_customizados():
    tema_customizado = go.layout.Template(
        layout=dict(
            font=dict(family="Arial, sans-serif", color="#333"),
            title=dict(font=dict(family="Arial, sans-serif", size=22, color="#1E3A8A"), x=0.5),
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            colorway=px.colors.qualitative.Plotly,
            legend=dict(
                bgcolor="rgba(255,255,255,0.6)",
                bordercolor="#E5E7EB",
                borderwidth=1
            ),
            xaxis=dict(gridcolor="#E5E7EB"),
            yaxis=dict(gridcolor="#E5E7EB"),
            hoverlabel=dict(
                bgcolor="#1E3A8A",
                font_family="Arial, sans-serif",
                font_color="white"
            ),
        )
    )
    px.defaults.template = tema_customizado

    st.markdown("""
    <style>
        .main-header {
            font-size: 2.5rem;
            color: #1E3A8A;
            text-align: center;
            margin-bottom: 1.5rem;
        }
        .sub-header {
            font-size: 1.75rem;
            color: #1E3A8A;
            margin-top: 2rem;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #DBEAFE;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 5px;
            border-bottom: 2px solid #DBEAFE;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            background-color: #F3F4F6;
            border-radius: 8px 8px 0 0;
            padding: 10px 20px;
            transition: all 0.2s ease-in-out;
        }
        .stTabs [aria-selected="true"] {
            background-color: #DBEAFE;
            color: #1E3A8A;
            font-weight: bold;
            border-bottom: 2px solid #2563EB;
        }
        .footer {
            text-align: center;
            margin-top: 3rem;
            padding: 1rem;
            border-top: 1px solid #E5E7EB;
            color: #6B7280;
            font-size: 0.9rem;
        }
        .filter-summary {
            background-color: #F3F4F6;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource(ttl=3600)
def obter_engine_db():
    try:
        db_credentials = st.secrets["postgres"]
        conn_string = (
            f"postgresql+psycopg2://{db_credentials['user']}:{db_credentials['password']}"
            f"@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['database']}"
        )
        return create_engine(conn_string)
    except Exception as e:
        st.error(f"Falha na conexão com o banco de dados: {e}")
        return None

@st.cache_data(ttl=3600)
def carregar_dados_db(_engine, query, params=None):
    if _engine is None:
        return pd.DataFrame()
    try:
        with _engine.connect() as connection:
            df = pd.read_sql_query(sql=text(query), con=connection, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

class DashboardPIB:
    def __init__(self):
        self.engine = obter_engine_db()
        if self.engine is None:
            st.stop()
        
        if 'initialized' not in st.session_state:
            self.inicializar_estado()

    def inicializar_estado(self):
        with st.spinner("Carregando dados iniciais..."):
            st.session_state.anos_disponiveis = self.obter_anos_disponiveis()
            st.session_state.ufs_df = self.obter_ufs_disponiveis()
            st.session_state.initialized = True
            
    def obter_anos_disponiveis(self):
        query = "SELECT DISTINCT ano_pib FROM pib_municipios ORDER BY ano_pib DESC"
        df = carregar_dados_db(self.engine, query)
        return [int(ano) for ano in df['ano_pib']] if not df.empty else []

    def obter_ufs_disponiveis(self):
        query = "SELECT cd_uf, sigla_uf, nome_uf FROM unidade_federacao ORDER BY sigla_uf"
        return carregar_dados_db(self.engine, query)
    
    def obter_municipios_por_ufs(self, ufs_selecionadas_siglas):
        if not ufs_selecionadas_siglas:
            return pd.DataFrame(columns=['codigo_municipio_dv', 'nome_municipio'])
            
        ufs_df = st.session_state.ufs_df
        cd_ufs = ufs_df[ufs_df['sigla_uf'].isin(ufs_selecionadas_siglas)]['cd_uf'].tolist()
        
        query = """
        SELECT codigo_municipio_dv, nome_municipio, municipio_capital
        FROM municipio 
        WHERE cd_uf IN :cd_ufs
        ORDER BY nome_municipio
        """
        return carregar_dados_db(self.engine, query, params={'cd_ufs': tuple(cd_ufs)})

    def obter_dados_pib_filtrados(self, anos, municipios_codigos):
        if not municipios_codigos or not anos:
            return pd.DataFrame()
        
        query = """
        SELECT 
            p.ano_pib, p.codigo_municipio_dv, p.vl_pib, p.vl_pib_per_capta,
            p.vl_agropecuaria, p.vl_industria, p.vl_servicos, p.vl_administracao,
            m.nome_municipio, m.municipio_capital, m.longitude, m.latitude,
            u.sigla_uf, u.nome_uf, u.cd_regiao
        FROM pib_municipios p
        JOIN municipio m ON p.codigo_municipio_dv = m.codigo_municipio_dv
        JOIN unidade_federacao u ON m.cd_uf = u.cd_uf
        WHERE CAST(p.ano_pib AS INTEGER) BETWEEN :ano_inicial AND :ano_final
        AND p.codigo_municipio_dv IN :municipios_codigos
        """
        params = {
            'ano_inicial': anos[0], 
            'ano_final': anos[1], 
            'municipios_codigos': tuple(municipios_codigos)
        }
        df = carregar_dados_db(self.engine, query, params=params)
        
        if not df.empty and 'populacao_estimada' not in df.columns:
            df['populacao_estimada'] = (df['vl_pib'] / df['vl_pib_per_capta']).fillna(0).astype(int)
        
        gc.collect()
        return df

    def exibir_barra_lateral(self):
        st.sidebar.markdown("<h2 style='text-align: center; color: #1E3A8A;'>Filtros</h2><hr>", unsafe_allow_html=True)
        
        anos = st.session_state.anos_disponiveis
        ufs_df = st.session_state.ufs_df

        if not anos:
            st.sidebar.error("Anos não encontrados.")
            st.stop()
            
        st.session_state.anos_selecionados = st.sidebar.slider(
            "Período (Ano)", min_value=min(anos), max_value=max(anos), 
            value=(max(anos) - 5, max(anos)), help="Selecione o intervalo de anos para a análise."
        )
        
        ufs_disponiveis = ufs_df['sigla_uf'].tolist()
        opcoes_filtro_uf = ["TODAS"] + ufs_disponiveis
        selecao_filtro_uf = st.sidebar.multiselect(
            "Estado(s) (UF)", options=opcoes_filtro_uf, default=["TODAS"],
            help="Selecione 'TODAS' para analisar o Brasil inteiro."
        )

        if "TODAS" in selecao_filtro_uf or not selecao_filtro_uf:
            st.session_state.ufs_selecionadas = ufs_disponiveis
        else:
            st.session_state.ufs_selecionadas = selecao_filtro_uf
        
        municipios_df = self.obter_municipios_por_ufs(st.session_state.ufs_selecionadas)
        municipios_disponiveis = sorted(municipios_df['nome_municipio'].unique())

        st.session_state.municipios_selecionados_nomes = st.sidebar.multiselect(
            "Município(s)", options=municipios_disponiveis, default=[],
            help="Deixe em branco para analisar todos do(s) estado(s) selecionado(s)."
        )

        if not st.session_state.municipios_selecionados_nomes:
            st.session_state.codigos_municipios_selecionados = municipios_df['codigo_municipio_dv'].tolist()
        else:
            st.session_state.codigos_municipios_selecionados = municipios_df[
                municipios_df['nome_municipio'].isin(st.session_state.municipios_selecionados_nomes)
            ]['codigo_municipio_dv'].tolist()
        
        st.session_state.tipo_visualizacao = st.sidebar.radio(
            "Visualizar por", ["PIB Total", "PIB Per Capita"], horizontal=True
        )
        
        with st.sidebar.expander("Opções de Visualização"):
            st.session_state.num_ranking = st.slider("Nº de Municípios no Ranking", 5, 30, 10)
            st.session_state.destacar_capitais = st.checkbox("Destacar Capitais 🏛️", True)

    def exibir_kpis(self, df):
        st.markdown("<h2 class='sub-header'>Indicadores Chave</h2>", unsafe_allow_html=True)

        if df.empty:
            st.info("Não há dados para os filtros selecionados.")
            return

        df['ano_pib'] = pd.to_numeric(df['ano_pib'])
        ano_inicial, ano_final = st.session_state.anos_selecionados
        df_ano_final = df[df['ano_pib'] == ano_final]
        df_ano_inicial = df[df['ano_pib'] == ano_inicial]

        if df_ano_final.empty:
            st.warning(f"Não há dados para o ano final ({ano_final}).")
            return
            
        pib_total_final = df_ano_final['vl_pib'].sum()
        pib_total_inicial = df_ano_inicial['vl_pib'].sum()
        delta_pib = None
        if ano_final != ano_inicial and pib_total_inicial > 0:
            delta_pib = f"{((pib_total_final - pib_total_inicial) / pib_total_inicial) * 100:.2f}%"

        pop_final = df_ano_final['populacao_estimada'].sum()
        pib_per_capita_medio = (pib_total_final / pop_final) if pop_final > 0 else 0
        num_municipios = df_ano_final['codigo_municipio_dv'].nunique()

        setores = {'Agropecuária': 'vl_agropecuaria', 'Indústria': 'vl_industria', 'Serviços': 'vl_servicos'}
        soma_setores = {nome: df_ano_final[col].sum() for nome, col in setores.items()}
        maior_setor = max(soma_setores, key=soma_setores.get) if soma_setores else "N/A"

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("PIB Total", f"R$ {pib_total_final/1e9:.2f} bi", delta=delta_pib, help=f"Variação de {ano_inicial} para {ano_final}")
        col2.metric("PIB Per Capita Médio", f"R$ {pib_per_capita_medio:,.2f}", help="Média ponderada pela população no último ano.")
        col3.metric("Municípios Analisados", f"{num_municipios}", help="Total de municípios na seleção para o último ano.")
        col4.metric("Setor Principal", maior_setor, help=f"Setor com maior contribuição em {ano_final}")

    def exibir_graficos(self, df):
        st.markdown("<h2 class='sub-header'>Visualizações Interativas</h2>", unsafe_allow_html=True)
        tab_titles = ["Evolução Temporal 📈", "Ranking de Municípios 🏆", "Composição Setorial 📊", "Análise Geográfica 🗺️", "Dados 📄"]
        tabs = st.tabs(tab_titles)

        df['ano_pib'] = pd.to_numeric(df['ano_pib'])
        ano_final = st.session_state.anos_selecionados[1]
        df_ano_final = df[df['ano_pib'] == ano_final].copy()
        
        with tabs[0]:
            self.renderizar_evolucao_temporal(df)
        with tabs[1]:
            self.renderizar_ranking_municipios(df_ano_final)
        with tabs[2]:
            self.renderizar_composicao_setorial(df_ano_final)
        with tabs[3]:
            self.renderizar_analise_geografica(df_ano_final)
        with tabs[4]:
            self.exibir_tabela_dados(df)

    def renderizar_evolucao_temporal(self, df):
        if df.empty: return
            
        tipo_vis = st.session_state.tipo_visualizacao
        y_col = 'vl_pib' if tipo_vis == "PIB Total" else 'vl_pib_per_capta'
        agg_func = 'sum' if tipo_vis == "PIB Total" else 'mean'
        df_evolucao = df.groupby('ano_pib', as_index=False).agg(valor=(y_col, agg_func))

        fig_area = px.area(df_evolucao, x='ano_pib', y='valor', title=f'Evolução do {tipo_vis}', markers=True)
        fig_area.update_layout(height=400, hovermode='x unified')
        st.plotly_chart(fig_area, use_container_width=True)
        
        if len(df_evolucao) > 1:
            df_evolucao['crescimento_%'] = df_evolucao['valor'].pct_change() * 100
            fig_barra = px.bar(df_evolucao.dropna(), x='ano_pib', y='crescimento_%', title='Taxa de Crescimento Anual (%)')
            fig_barra.update_layout(height=300)
            st.plotly_chart(fig_barra, use_container_width=True)
        
    def renderizar_ranking_municipios(self, df):
        if df.empty: return

        tipo_vis = st.session_state.tipo_visualizacao
        valor_col = 'vl_pib' if tipo_vis == "PIB Total" else 'vl_pib_per_capta'
        n = st.session_state.num_ranking
        
        df['display_name'] = df.apply(
            lambda row: f"{row['nome_municipio']} {'🏛️' if row['municipio_capital'] and st.session_state.destacar_capitais else ''}", axis=1
        )
        
        top_n = df.sort_values(valor_col, ascending=False).head(n)
        fig_top = px.bar(top_n, x=valor_col, y='display_name', orientation='h', title=f'Top {n} Municípios por {tipo_vis}', color='sigla_uf')
        fig_top.update_layout(yaxis_categoryorder='total ascending', height=max(400, n * 35), legend_title_text='UF')
        st.plotly_chart(fig_top, use_container_width=True)

        if st.checkbox("Mostrar municípios com menor PIB"):
            bottom_n = df.sort_values(valor_col, ascending=True).head(n)
            fig_bottom = px.bar(bottom_n, x=valor_col, y='display_name', orientation='h', title=f'Bottom {n} Municípios por {tipo_vis}', color='sigla_uf')
            fig_bottom.update_layout(yaxis_categoryorder='total descending', height=max(400, n * 35))
            st.plotly_chart(fig_bottom, use_container_width=True)

    def renderizar_composicao_setorial(self, df):
        if df.empty: return
        
        setores_cols = ['vl_agropecuaria', 'vl_industria', 'vl_servicos', 'vl_administracao']
        setores_nomes = ['Agropecuária', 'Indústria', 'Serviços', 'Adm. Pública']
        total_setorial = {nome: df[col].sum() for nome, col in zip(setores_nomes, setores_cols)}
        
        col1, col2 = st.columns([1, 1])
        with col1:
            df_pizza = pd.DataFrame(list(total_setorial.items()), columns=['Setor', 'Valor'])
            fig_pizza = px.pie(df_pizza, values='Valor', names='Setor', title='Distribuição Setorial Agregada', hole=0.4)
            fig_pizza.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pizza, use_container_width=True)
        
        with col2:
            df_bar_uf = df.groupby('sigla_uf')[setores_cols].sum().reset_index()
            df_bar_melted = df_bar_uf.melt(id_vars='sigla_uf', value_vars=setores_cols, var_name='setor', value_name='valor')
            df_bar_melted['setor'] = df_bar_melted['setor'].map(dict(zip(setores_cols, setores_nomes)))
            fig_bar = px.bar(df_bar_melted, x='sigla_uf', y='valor', color='setor', title='Composição do PIB por UF', barmode='stack')
            st.plotly_chart(fig_bar, use_container_width=True)

    def renderizar_analise_geografica(self, df):
        if df.empty: return
        tipo_vis = st.session_state.tipo_visualizacao
        valor_col = 'vl_pib' if tipo_vis == "PIB Total" else 'vl_pib_per_capta'
        
        df_geo = df.dropna(subset=['latitude', 'longitude'])
        if not df_geo.empty:
            fig_map = px.scatter_mapbox(df_geo, lat='latitude', lon='longitude', color=valor_col, size=valor_col,
                hover_name='nome_municipio', size_max=40, zoom=3.5, height=600, title=f'Distribuição Geográfica do {tipo_vis}',
                color_continuous_scale=px.colors.cyclical.IceFire)
            
            # Remove a legenda de cor (colorbar)
            fig_map.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0, "t":40, "l":0, "b":0},
                coloraxis_showscale=False 
            )
            st.plotly_chart(fig_map, use_container_width=True)
        
    def exibir_tabela_dados(self, df):
        st.markdown("### Dados Detalhados")
        st.dataframe(df, use_container_width=True)
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download (CSV)",
            data=csv,
            file_name=f"pib_municipios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    def executar(self):
        st.markdown("<h1 class='main-header'>📊 Dashboard do PIB dos Municípios Brasileiros</h1>", unsafe_allow_html=True)
        
        self.exibir_barra_lateral()

        if not st.session_state.codigos_municipios_selecionados:
            st.warning("Nenhum município selecionado. Por favor, ajuste os filtros da barra lateral.")
            return # Usa return para parar a execução de forma limpa
            
        with st.spinner("Carregando e processando dados..."):
            df_filtrado = self.obter_dados_pib_filtrados(
                st.session_state.anos_selecionados, 
                st.session_state.codigos_municipios_selecionados
            )
        
        uf_display = "TODAS" if len(st.session_state.ufs_selecionadas) >= len(st.session_state.ufs_df) else ', '.join(st.session_state.ufs_selecionadas)
        mun_display = "TODOS" if not st.session_state.municipios_selecionados_nomes else f"{len(st.session_state.municipios_selecionados_nomes)} selecionado(s)"
        st.markdown(f"""
        <div class='filter-summary'>
            <strong>Filtros Ativos:</strong> Período: {st.session_state.anos_selecionados[0]}-{st.session_state.anos_selecionados[1]} | 
            UF(s): {uf_display} | 
            Município(s): {mun_display}
        </div>
        """, unsafe_allow_html=True)

        if df_filtrado.empty:
            st.error("Nenhum dado encontrado para a combinação de filtros. Tente uma seleção diferente.")
        else:
            self.exibir_kpis(df_filtrado)
            self.exibir_graficos(df_filtrado)
            
        st.markdown(f"<div class='footer'>Dashboard PIB Municípios | {datetime.now().strftime('%d/%m/%Y')}</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    configurar_pagina()
    aplicar_estilos_customizados()
    app = DashboardPIB()
    app.executar()
