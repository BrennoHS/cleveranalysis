"""
ai_explainer.py — Generates a professional discrepancy explanation using Gemini.
"""

import os
import google.generativeai as genai
from dotenv import load_dotenv
from analysis import DiscrepancyResult

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=True)

genai.configure(api_key=os.environ["GEMINI_API_KEY"])

EXPLAINER_MODEL = os.environ.get("GEMINI_EXPLAINER_MODEL", "gemini-flash-lite-latest")
EXPLAINER_MAX_OUTPUT_TOKENS = int(os.environ.get("GEMINI_EXPLAINER_MAX_OUTPUT_TOKENS", "320"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


EXPLAINER_ENABLED = _env_bool("GEMINI_EXPLAINER_ENABLED", True)


EXPLANATION_PROMPT = """
Você é um analista sênior de AdOps escrevendo um relatório profissional de discrepâncias para um time interno.
Responda exclusivamente em português do Brasil.

Aqui estão os dados de comparação entre o relatório do publisher e nosso servidor de ads interno Clever:

Dados do Publisher:
- Impressões Servidas: {pub_served:,}
- Impressões Visualizáveis: {pub_viewable:,}
- Taxa de Visualização: {pub_viewability:.1f}%

Dados Clever (Interno):
- Impressões Servidas: {clever_served:,}
- Impressões Visualizáveis (Ajustadas): {clever_viewable:,}
- Garbage: {clever_garbage:,}
- Taxa de Visualização: {clever_viewability:.1f}%

Métricas de Discrepância:
- Diferença em Impressões Servidas: {diff_served:,}
- Diferença em Impressões Visualizáveis: {diff_viewable:,}
- Diferença de Visualização: {viewability_diff_pp:+.1f} pontos percentuais
- Status: {status}

Período: {start_date} a {end_date}
Publisher / Domínio: {publisher}

Escreva uma análise profissional de 2–3 parágrafos que:
1. Descreva claramente o que os dados mostram e se a discrepância está dentro de um intervalo aceitável
2. Compare ambos os lados de forma objetiva
3. Sugira 2–3 possíveis causas realistas (por exemplo, posicionamento da tag de rastreamento, implementação em source page que leva a publicidade a entrar em CAP no primeiro acesso, possível má implementação do lado do publisher, etc.)

Mantenha o tom profissional e factual. NÃO invente detalhes técnicos específicos não suportados pelos dados. NÃO use bullet points ou formatações adicionais — escreva em parágrafos fluidos.
"""


def _deterministic_fallback_explanation(
    result: DiscrepancyResult,
    start_date: str,
    end_date: str,
    publisher: str,
) -> str:
    """Local Portuguese fallback used when Gemini quota/rate limits are reached."""
    pub_view_pct = result.pub_viewability * 100
    clever_view_pct = result.clever_viewability * 100
    view_pp = result.viewability_diff_pp
    view_pp_sign = "+" if view_pp >= 0 else ""

    mode_label = "modo IA desativada" if not EXPLAINER_ENABLED else "modo contingência"
    return (
        f"Resumo automático ({mode_label}): no período de {start_date} a {end_date}, "
        f"o publisher {publisher or 'não especificado'} reportou {result.pub_served:,} served e "
        f"{result.pub_viewable:,} viewable, enquanto a Clever registrou {result.clever_served:,} served e "
        f"{result.clever_viewable:,} viewable (já considerando garbage). "
        f"A discrepância percentual ficou em {result.served_discrepancy_pct:.2f}% para served e "
        f"{result.viewable_discrepancy_pct:.2f}% para viewable.\n\n"
        f"Em viewability, o publisher ficou em {pub_view_pct:.1f}% e a Clever em {clever_view_pct:.1f}%, "
        f"com diferença de {view_pp_sign}{view_pp:.1f} p.p. O status atual está classificado como "
        f"{result.status}. Possíveis causas incluem diferenças de metodologia de medição, janela de contagem, "
        f"filtros de tráfego inválido e timing de renderização/tagueamento entre as plataformas."
    )


def _is_quota_or_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    markers = (
        "429",
        "quota",
        "rate limit",
        "exceeded",
        "generativelanguage.googleapis.com",
    )
    return any(m in message for m in markers)


def generate_explanation(
    result: DiscrepancyResult,
    start_date: str,
    end_date: str,
    publisher: str,
    suspicious_analysis: dict | None = None,
) -> str:
    """
    Calls Gemini to generate a professional explanation of the discrepancy.
    Returns a plain text explanation string.
    """
    if not EXPLAINER_ENABLED:
        return _deterministic_fallback_explanation(result, start_date, end_date, publisher)

    model = genai.GenerativeModel(EXPLAINER_MODEL)

    prompt = EXPLANATION_PROMPT.format(
        pub_served=result.pub_served,
        pub_viewable=result.pub_viewable,
        pub_viewability=result.pub_viewability * 100,
        clever_served=result.clever_served,
        clever_viewable=result.clever_viewable,
        clever_garbage=result.clever_garbage,
        clever_viewability=result.clever_viewability * 100,
        diff_served=result.diff_served,
        diff_viewable=result.diff_viewable,
        viewability_diff_pp=result.viewability_diff_pp,
        status=result.status,
        start_date=start_date,
        end_date=end_date,
        publisher=publisher or "Not specified",
    )

    if suspicious_analysis and suspicious_analysis.get("enabled"):
        metrics = suspicious_analysis.get("metrics", {})
        overall = suspicious_analysis.get("overall", {})
        served = suspicious_analysis.get("served_analysis", {})
        viewable = suspicious_analysis.get("viewable_analysis", {})
        flags = suspicious_analysis.get("flags", [])
        top_ips = metrics.get("top_10_ips", [])[:5]
        top_ip_text = ", ".join(f"{i.get('value')} ({i.get('count')})" for i in top_ips) if top_ips else "n/a"
        flags_text = "; ".join(flags) if flags else "nenhum gatilho crítico"

        prompt += (
            "\n\nAnálise determinística adicional de tráfego suspeito (checkbox ativado):\n"
            f"- Risk score: {suspicious_analysis.get('risk_score', 'n/a')}/100\n"
            f"- Risk level: {suspicious_analysis.get('risk_level', 'n/a')}\n"
            f"- % top 10 IPs (geral): {overall.get('top10_ip_concentration_pct', 'n/a')}\n"
            f"- % mesmo IP (geral): {overall.get('same_ip_traffic_pct', metrics.get('same_ip_traffic_pct', 'n/a'))}\n"
            f"- % diversidade de IPs (geral): {overall.get('ip_diversity_pct', 'n/a')}\n"
            f"- % concentração por região (top): {overall.get('top_region_pct', 'n/a')}\n"
            f"- Diversidade de resoluções (únicas): {overall.get('unique_resolution_total', 'n/a')}\n"
            f"- Registros >20/dia (geral): {overall.get('records_over_20_per_day', 'n/a')}\n"
            f"- Registros >50/dia (geral): {overall.get('records_over_50_per_day', 'n/a')}\n"
            f"- Registros >100/dia (geral): {overall.get('records_over_100_per_day', 'n/a')}\n"
            f"- Usuários repetindo >20/dia: {overall.get('repeat_users_over_20_days', 'n/a')}\n"
            f"- Pico máximo usuário/dia: {overall.get('max_events_single_user_day', 'n/a')}\n"
            f"- Sinais críticos simultâneos: {overall.get('critical_signal_count', 'n/a')}\n"
            f"- Tempo uniforme (CV): {overall.get('time_uniformity_cv', metrics.get('time_uniformity_cv', 'n/a'))}\n"
            f"- Served -> % top 10 IPs: {served.get('top10_ip_concentration_pct', 'n/a')}, "
            f"tempo médio por usuário (s): {served.get('avg_seconds_between_events_per_user', 'n/a')}\n"
            f"- Viewable -> % top 10 IPs: {viewable.get('top10_ip_concentration_pct', 'n/a')}, "
            f"tempo médio por usuário (s): {viewable.get('avg_seconds_between_events_per_user', 'n/a')}\n"
            f"- Top IPs: {top_ip_text}\n"
            f"- Flags: {flags_text}\n\n"
            "No final da análise, inclua um parecer objetivo sobre risco de tráfego inválido com uma classificação clara"
            " (baixo/moderado/alto) e recomendação operacional (ex.: monitorar, investigar profundamente, pausar campanha)."
        )

    try:
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.2,
                max_output_tokens=EXPLAINER_MAX_OUTPUT_TOKENS,
            ),
        )
        text = (response.text or "").strip()
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            return _deterministic_fallback_explanation(result, start_date, end_date, publisher)
        raise

    if not text:
        return _deterministic_fallback_explanation(result, start_date, end_date, publisher)

    return text
