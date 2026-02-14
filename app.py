import dash
import dash_bootstrap_components as dbc
from dash import dcc, Input, Output, html, callback, State
import plotly.express as px
import pandas as pd
from dotenv import load_dotenv
from data_loader import (
    load_sales,
    load_customers,
    load_payment_terms,
    load_failures_by_city,
    load_ventas_detfechas,
    load_failures_by_city_wparams,
    montoventas_xtiponegocio
)  # importar funciones de consulta base de datos
from datetime import date


"""
def load_data():

    data = load_data()
 """

# Creating a Web App


app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
server = app.server 

# Datos de ejemplo (ventas maquinaria agroforestal)

df_prueba = pd.DataFrame(
    {
        "fecha": ["2025-01", "2025-02", "2025-03", "2025-04"],
        "ventas": [10000, 15000, 12000, 18000],
        "clientes": [50, 75, 60, 90],
        "region": ["Norte", "Sur", "Norte", "Sur"],
    }
)


# df = load_sales()


# Navbar profesional
navbar = dbc.NavbarSimple(
    #children=[
        #dbc.NavItem(dbc.NavLink("📊 Dashboard", href="#", active="exact")),
       # dbc.NavItem(dbc.NavLink("📈 Graficos", href="#ventas")),
       # dbc.NavItem(dbc.NavLink("👥 Estadistica", href="#clientes")),
        #dbc.NavItem(dbc.NavLink("⚙️ Reportes", href="#reportes")),
    #],
    brand="🚜 AgroAnalytics",
    brand_href="#",
    color="dark",
    dark=True,
    className="mb-4",
)
# Layout principal
app.layout = html.Div(
    [
        navbar,
        dbc.Container(
            [
                dbc.Tabs(
                    [
                        dbc.Tab(label="📊 Dashboard", tab_id="dashboard"),
                        dbc.Tab(label="📈 Fallas-Equipos", tab_id="ventas"),
                       # dbc.Tab(label="👥 Clientes", tab_id="clientes"),
                        dbc.Tab(label="⚙️ Reportes", tab_id="reportes"),
                        # html.Div(id="date-picker-container", style={"display": "none"}),
                    ],
                    id="tabs",
                    active_tab="dashboard",
                ),
                # html.Div(id="date-picker-container", style={"display": "none"}),
                # Contenido dinámico
                dcc.DatePickerRange(
                    id="my-date-picker-range",
                    start_date=date(2020, 1, 1),
                    end_date=date(2025, 12, 31),
                    display_format="DD-MM-YYYY",  # Formato día-mes-año
                    start_date_placeholder_text="DD-MM-YYYY",
                    end_date_placeholder_text="DD-MM-YYYY",
                    first_day_of_week=1,  # Lunes como primer día de la semana
                ),
                html.Div(id="output-container-date-picker-range"),
                html.Div(id="tab-content", className="mt-4"),
            ],
            fluid=True,
        ),
    ]
)


# Callbacks (interactividad)
@callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab"),
    Input("my-date-picker-range", "start_date"),
    Input("my-date-picker-range", "end_date"),
    # dash.State("dropdowntot_fallas", "value"),
    # prevent_initial_call=False
)
def render_tab_content(active_tab, start_date, end_date):
    # Fechas por defecto
    try:
        df = load_sales()
        print(f"DEBUG: df shape = {df.shape}")
    except:
        df = pd.DataFrame(
            {
                "vendedor": ["Laura", "Eduardo", "Maria"],
                "monto_vendido": [15000, 12000, 18000],
            }
        )

    if active_tab == "dashboard":
        total = df["monto_vendido"].sum()
        lider = str(df["vendedor"].iloc[0]).title()
        # Validar que las fechas estén presentes
        if not start_date or not end_date:
            return {"data": [], "layout": {"title": "Seleccioná un rango de fechas"}}
        try:
            start_date = date.fromisoformat(start_date.split("T")[0])
            end_date = date.fromisoformat(end_date.split("T")[0])
        except (ValueError, TypeError):
            return {"data": [], "layout": {"title": "Formato de fecha inválido"}}

        df_formapago = load_payment_terms(start_date, end_date)

        # ✅ TABLA CORREGIDA (NO iterrows!)
        table_rows = []
        for i in range(min(3, len(df))):
            row = df.iloc[i]
            table_rows.append(
                html.Tr(
                    [
                        html.Td(str(i + 1)),
                        html.Td(str(row["vendedor"]).title()),
                        html.Td(f"${int(row['monto_vendido']):,.0f}"),
                    ]
                )
            )
        return html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H1(
                                    "¡TOP Vendedores x Monto Venta!",
                                    className="text-center text-primary mb-5",
                                )
                            ],
                            width=12,
                        )
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.P(
                                    f"🏆 Líder: {lider}", className="fs-4 text-success"
                                ),
                                html.P(
                                    f"💰 TOP 3-Total Vendido: ${total:,.0f}",
                                    className="fs-3",
                                ),
                            ],
                            width=6,
                        ),
                        dbc.Col(
                            [
                                html.H4(
                                    "Top 3 Vendedores", className="text-center mb-4"
                                ),
                                dbc.Table(
                                    [
                                        html.Thead(
                                            html.Tr(
                                                [
                                                    html.Th("Rango"),
                                                    html.Th("Vendedor"),
                                                    html.Th("Monto ($)"),
                                                ]
                                            )
                                        ),
                                        html.Tbody(table_rows),
                                    ],
                                    striped=True,
                                    bordered=True,
                                    hover=True,
                                    responsive=True,
                                    className="shadow-sm",
                                ),
                            ],
                            width=6,
                        ),
                    ],
                    className="mb-4",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H3(
                                    "📊 Distribución por Forma de Pago",
                                    className="text-center mb-3",
                                ),
                                dcc.Graph(
                                    figure=px.pie(
                                        df_formapago,
                                        values="cantidad_ventas",
                                        names="forma_pago",
                                        title="PORC VENTAS POR FORMA DE PAGO",
                                        height=400,
                                    ).update_layout(height=400),
                                    style={"height": "450px"},
                                ),
                            ],
                            width=12,
                        )
                    ]
                ),
            ]
        )
    elif active_tab == "ventas":
        df_fallas_wparams = load_failures_by_city_wparams(None)

        # Obtenemos las opciones únicas para el dropdown
        opciones = sorted(df_fallas_wparams["total_fallas"].unique())
        ##df_fallas_wparams = load_failures_by_city_wparams(total_falla)
        df_fallas = load_failures_by_city()
        df_fallas["ciudad"] = df_fallas["ciudad"].str.title()
        return dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("filtro por cantidad de fallas"),
                        dcc.Dropdown(
                            id="dropdowntot_fallas",
                            options=[{"label": str(i), "value": i} for i in opciones],
                            value=None,  # Mantiene el valor seleccionado
                            placeholder="cantidad de fallas",
                        ),
                        html.Hr(),
                        # Contenedor para los resultados (gráfico o tabla)
                        # html.Div(id='resultado-busqueda-fallas')
                    ],
                    width=12,
                ),
                dbc.Col([html.Div(id="resultado-busqueda-fallas")], width=12),
            ]
     
        )
    elif active_tab == "reportes":
        """
        df_fallas_wparams = load_failures_by_city_wparams(None)

        # Obtenemos las opciones únicas para el dropdown
        opciones = sorted(df_fallas_wparams["total_fallas"].unique())
        ##df_fallas_wparams = load_failures_by_city_wparams(total_falla)
        df_fallas = load_failures_by_city()
        df_fallas["ciudad"] = df_fallas["ciudad"].str.title()
        """
        return dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("Monto Ventas Por Tipo_negocio y fecha"),
                        
                        html.Hr(),
                        dcc.DatePickerSingle(
                            id='date-pickersingle',
                            display_format="DD-MM-YYYY",  # Formato día-mes-año
                            date=date(2020, 1, 1),  # Fecha inicial
                            min_date_allowed='2020-01-01',
                            max_date_allowed='2026-12-31',
                            first_day_of_week=1,  # Lunes como primer día de la semana
                        ),
                        # Contenedor para los resultados (gráfico o tabla)
                        # html.Div(id='resultado-busqueda-fallas')
                    ],
                    width=12,
                ),
                dbc.Col([html.Div(id="montoventas-tiponegocio")], width=12),
            ]
     
        )


@callback(
    Output("resultado-busqueda-fallas", "children"),
    Input("dropdowntot_fallas", "value"),
    prevent_initial_call=False,
)
def update_results_dropdown(total_falla):
    # Aquí aplicas tu consulta con parámetros
    df = load_failures_by_city_wparams(total_falla)

    if df.empty:
        return dbc.Alert("No hay datos para este filtro", color="warning")
    df["ciudad"] = df["ciudad"].str.title()
    fig = px.bar(
        df.head(10).sort_values("total_fallas", ascending=True),
        x="total_fallas",
        y="ciudad",
        orientation="h",
        color="total_fallas",
        color_continuous_scale="Reds",
        title="🚨 CIUDADES CON FALLAS DE EQUIPOS"
        if total_falla is None
        else f"🚨 CIUDADES CON {total_falla} FALLAS",
    ).update_layout(height=650)
    # 3. Retornamos el componente gráfico al Div 'resultado-busqueda-fallas'
    return dcc.Graph(figure=fig, style={"height": "650px"})

@callback(
    Output("montoventas-tiponegocio", "children"),
    Input("date-pickersingle", "date"),
    prevent_initial_call=False,
)

def update_histogram(selected_date):
    if not selected_date:
         # ✅ Retorna componente vacío válido
        return html.Div("Seleccioná una fecha", style={"textAlign": "center", "padding": "50px"})
            ##return {"data": [], "layout": {"title": "Seleccioná un rango de fechas"}}  # Gráfico vacío inicial
    try:
        selected_date = date.fromisoformat(selected_date.split("T")[0])
            ##end_date = date.fromisoformat(end_date.split("T")[0])
    except (ValueError, TypeError):
            #return {"data": [], "layout": {"title": "Formato de fecha inválido"}}
            return html.Div("Formato de fecha inválido", style={"color": "red"})
    df_montonegocio = montoventas_xtiponegocio(selected_date)  # Retorna df con tipo_negocio y total_ingresos
    print(df_montonegocio)
    fig = px.histogram(
        df_montonegocio,
        x='tipo_negocio',
        y='total_ingresos',
        title=f"Ingresos por tipo de negocio desde {selected_date.strftime('%d-%m-%Y')}",
        histfunc='sum',  # Suma totales (ideal para agregados)
        color='tipo_negocio'  # Colores por categoría
    )
    fig.update_traces(texttemplate="$%{y:,.0f}")
    fig.update_layout(bargap=0.2, height=650, yaxis=dict(tickformat="$,.0f"))
    return dcc.Graph(figure=fig, style={"height": "650px"})
   


if __name__ == "__main__":
    app.run(debug=True, port=8050)
"""
 fig = px.bar(
            df_fallas.head(10).sort_values("ciudad", ascending=False),
            x="total_fallas", y="ciudad", orientation="h",
            color="total_fallas", color_continuous_scale="Reds",
            title="🚨 TOP 10 CIUDADES con MÁS FALLAS DE EQUIPOS"
        ).update_layout(height=650)
        
        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig, style={"height": "650px"})
            ], width=12),
"""
