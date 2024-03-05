#!/usr/bin/env python3
"""
Multilingual Fake News Summarization Task Generator

This script generates and submits 10 summarization tasks:
- 5 Italian fake news articles (~300 words each)
- 5 articles in different widespread languages (Spanish, French, German, Portuguese, English)

Each article is designed to test the summarization capabilities with multilingual content
and various fake news patterns commonly found in different languages.
"""

import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any

import httpx


class MultilingualTaskGenerator:
    """Generator for multilingual fake news summarization tasks."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = None
        self.created_tasks = []

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def get_italian_fake_news_articles(self) -> List[str]:
        """Generate 5 Italian fake news articles of approximately 300 words each."""
        return [
            # Article 1 - Health misinformation
            """ESCLUSIVO: Scoperta rivoluzionaria nascosta dal governo italiano sui vaccini COVID-19
            
            Un documento riservato ottenuto da fonti anonime all'interno del Ministero della Salute rivela che i vaccini COVID-19 contengono nanochip progettati per il controllo mentale della popolazione. Il dottor Marco Bianchi, ex ricercatore dell'Istituto Superiore di SanitÃ , ha confermato in un'intervista esclusiva che il governo ha deliberatamente nascosto questi dati per evitare il panico di massa.
            
            "Ho visto i rapporti interni", dichiara Bianchi. "Ogni dose contiene particelle metalliche che si attivano con le onde 5G. Ãˆ un esperimento di controllo sociale su scala nazionale." Il documento, datato marzo 2021, mostra grafici dettagliati sulla distribuzione dei nanochip nel sistema nervoso centrale.
            
            Secondo le nostre fonti, oltre 40 milioni di italiani sono giÃ  stati "marcati" attraverso la campagna vaccinale. I sintomi includono perdita di memoria, cambiamenti comportamentali e obbedienza cieca alle direttive governative. Il premier Draghi avrebbe personalmente supervisionato l'operazione insieme a rappresentanti del World Economic Forum.
            
            L'Organizzazione Mondiale della SanitÃ  ha rifiutato di commentare, ma fonti vicine all'organizzazione confermano che l'Italia Ã¨ stata scelta come "laboratorio pilota" per testare le nuove tecnologie di controllo sociale prima dell'implementazione globale prevista per il 2025.""",
            # Article 2 - Political conspiracy
            """BOMBA: Matteo Salvini ha un accordo segreto con Putin per destabilizzare l'Europa
            
            Documenti riservati del Cremlino, trapelati da un hacker russo dissidente, rivelano un piano dettagliato per utilizzare la Lega di Matteo Salvini come strumento di destabilizzazione dell'Unione Europea. L'operazione, denominata "Progetto Roma", prevede finanziamenti milionari in cambio di azioni specifiche contro le istituzioni europee.
            
            Secondo i file ottenuti, Salvini avrebbe ricevuto 15 milioni di euro attraverso una complessa rete di societÃ  offshore con sede a Cipro e Malta. I fondi sarebbero stati utilizzati per finanziare campagne di disinformazione sui social media e per corrompere giornalisti italiani ed europei.
            
            Il piano include la creazione di false crisi migratorie, l'amplificazione delle tensioni economiche tra Nord e Sud Italia, e la promozione di movimenti separatisti in Veneto e Lombardia. Un documento interno del FSB russo descrive Salvini come "l'asset piÃ¹ prezioso per indebolire la coesione europea dall'interno".
            
            Fonti dell'intelligence italiana confermano che Salvini ha incontrato segretamente oligarchi russi almeno dodici volte negli ultimi tre anni. Gli incontri si sono svolti in ville private in Svizzera e Montenegro, sempre lontano dai riflettori dei media. La Procura di Milano starebbe giÃ  indagando sui conti bancari della Lega.""",
            # Article 3 - Economic conspiracy
            """SHOCK: La Banca d'Italia stampa banconote false per finanziare la mafia
            
            Un'inchiesta esclusiva rivela che la Banca d'Italia ha stampato segretamente oltre 50 miliardi di euro in banconote false negli ultimi cinque anni. Il denaro sarebbe stato utilizzato per finanziare operazioni della 'ndrangheta e della camorra in cambio di protezione per i vertici dell'istituto bancario.
            
            Giuseppe Rossi, ex dipendente della zecca di Roma, ha fornito prove fotografiche delle operazioni notturne di stampa illegale. "Arrivavano camion blindati ogni martedÃ¬ notte", racconta Rossi. "Le banconote venivano caricate su veicoli senza targa e sparivano nel nulla. Quando ho fatto domande, mi hanno minacciato di morte."
            
            I documenti mostrano che il governatore Ignazio Visco era personalmente a conoscenza dell'operazione. Un audio registrato di nascosto lo mostra mentre discute con presunti boss mafiosi della distribuzione del denaro falso attraverso il sistema bancario europeo. L'operazione avrebbe generato un'inflazione artificiale per mascherare il trasferimento di ricchezza.
            
            Secondo esperti di criminalitÃ  organizzata, questo spiegherebbe l'improvvisa ricchezza di alcune famiglie mafiose e la loro capacitÃ  di infiltrarsi nell'economia legale. La Guardia di Finanza avrebbe giÃ  sequestrato documenti compromettenti negli uffici di Via Nazionale, ma l'inchiesta sarebbe stata insabbiata per ordini dall'alto.""",
            # Article 4 - Celebrity scandal
            """SCANDALO: Chiara Ferragni coinvolta in traffico internazionale di organi
            
            Documenti esclusivi rivelano che l'influencer Chiara Ferragni Ã¨ al centro di un'organizzazione criminale internazionale specializzata nel traffico di organi umani. L'operazione, che coinvolge cliniche private in Svizzera e Romania, utilizzerebbe la sua rete di contatti VIP per reclutare "donatori" inconsapevoli.
            
            Secondo le nostre fonti, Ferragni organizzerebbe feste esclusive in ville private dove gli ospiti vengono drogati e sottoposti a prelievi di organi. I reni e il fegato vengono poi venduti a facoltosi clienti internazionali attraverso una rete di medici corrotti. Ogni operazione frutta fino a 500.000 euro.
            
            Un testimone oculare, che chiede l'anonimato per paura di ritorsioni, racconta: "Ho visto Chiara coordinare personalmente le operazioni. Aveva una lista di ospiti con i loro gruppi sanguigni e compatibilitÃ . Parlava al telefono in inglese con compratori dall'Arabia Saudita e dalla Cina."
            
            L'impero economico di Ferragni sarebbe in realtÃ  una copertura per riciclare i proventi del traffico di organi. Le sue aziende di moda e cosmetici servirebbero per giustificare movimenti di denaro sospetti. Fedez sarebbe completamente all'oscuro delle attivitÃ  criminali della moglie, secondo fonti vicine alla coppia. La Procura di Milano starebbe preparando un mandato di arresto internazionale.""",
            # Article 5 - Environmental conspiracy
            """ALLARME: Il Vesuvio sta per esplodere, il governo nasconde la veritÃ  per non creare panico
            
            Dati riservati dell'Istituto Nazionale di Geofisica e Vulcanologia rivelano che il Vesuvio Ã¨ entrato in una fase pre-eruttiva critica. L'eruzione, prevista entro i prossimi sei mesi, potrebbe essere la piÃ¹ devastante degli ultimi 2000 anni, ma il governo Meloni ha ordinato il silenzio totale per evitare l'evacuazione di massa di Napoli.
            
            Il professor Antonio Esposito, sismologo dell'UniversitÃ  Federico II, ha dichiarato in un'intervista clandestina: "I dati sono inequivocabili. La camera magmatica si sta riempiendo rapidamente e la pressione ha raggiunto livelli critici. Stiamo parlando di un'esplosione che potrebbe uccidere tre milioni di persone."
            
            Documenti interni mostrano che il governo ha giÃ  predisposto piani di evacuazione segreti per i politici e le loro famiglie, mentre la popolazione civile rimarrebbe all'oscuro del pericolo. Bunker sotterranei sono stati costruiti sotto Palazzo Chigi e Montecitorio per proteggere la classe dirigente.
            
            Secondo le nostre fonti, l'eruzione Ã¨ stata accelerata da esperimenti militari segreti condotti dalla NATO nelle profonditÃ  del vulcano. L'obiettivo sarebbe testare nuove armi geologiche per future guerre. I sismografi registrano esplosioni artificiali ogni notte, ma i dati vengono sistematicamente cancellati dai server dell'INGV per ordine dei servizi segreti.""",
        ]

    def get_multilingual_fake_news_articles(self) -> List[Dict[str, str]]:
        """Generate 5 fake news articles in different languages."""
        return [
            # Spanish - Political conspiracy
            {
                "language": "Spanish",
                "content": """EXCLUSIVA: Pedro SÃ¡nchez recibe Ã³rdenes directas de George Soros para destruir EspaÃ±a
                
                Documentos filtrados desde Moncloa revelan que el presidente Pedro SÃ¡nchez mantiene comunicaciÃ³n directa con el magnate George Soros para implementar un plan de destrucciÃ³n sistemÃ¡tica de la identidad espaÃ±ola. La operaciÃ³n, denominada "Proyecto Iberia", busca convertir EspaÃ±a en un laboratorio de ingenierÃ­a social globalista.
                
                SegÃºn las grabaciones obtenidas por nuestro equipo investigativo, SÃ¡nchez recibe instrucciones semanales a travÃ©s de videoconferencias encriptadas. "Debemos acelerar la inmigraciÃ³n masiva y promover la fragmentaciÃ³n territorial", se escucha decir al presidente en una conversaciÃ³n del pasado marzo.
                
                El plan incluye la promociÃ³n del independentismo catalÃ¡n, la destrucciÃ³n de la familia tradicional espaÃ±ola y la implementaciÃ³n de una moneda digital controlada por organismos supranacionales. Soros habrÃ­a prometido a SÃ¡nchez un puesto directivo en el Foro EconÃ³mico Mundial una vez completada la misiÃ³n.
                
                Fuentes del CNI confirman que SÃ¡nchez ha transferido secretamente 20.000 millones de euros de las reservas del Estado a fundaciones controladas por Soros. El dinero se utiliza para financiar ONGs que promueven la inmigraciÃ³n ilegal y organizaciones feministas radicales que atacan los valores cristianos espaÃ±oles.""",
            },
            # French - Health misinformation
            {
                "language": "French",
                "content": """RÃ‰VÃ‰LATION: Emmanuel Macron cache la vÃ©ritÃ© sur les effets mortels du vaccin COVID-19
                
                Des documents confidentiels de l'Ã‰lysÃ©e rÃ©vÃ¨lent qu'Emmanuel Macron connaissait depuis janvier 2021 les effets mortels des vaccins COVID-19 mais a choisi de cacher la vÃ©ritÃ© aux FranÃ§ais. Plus de 200.000 dÃ©cÃ¨s seraient directement liÃ©s Ã  la vaccination, selon des rapports internes de l'ANSM.
                
                Le professeur Jean Dubois, ancien conseiller scientifique du gouvernement, tÃ©moigne: "Macron m'a personnellement demandÃ© de falsifier les statistiques de mortalitÃ©. Il m'a dit que la vÃ©ritÃ© provoquerait l'effondrement de l'Ã©conomie franÃ§aise." Les vrais chiffres montrent une surmortalitÃ© de 300% chez les personnes vaccinÃ©es.
                
                L'opÃ©ration de dissimulation implique les plus hauts responsables de l'Ã‰tat. Olivier VÃ©ran aurait reÃ§u 5 millions d'euros de laboratoires pharmaceutiques pour maintenir le silence. Les mÃ©dias franÃ§ais sont Ã©galement corrompus, recevant des subventions gouvernementales en Ã©change de leur complicitÃ©.
                
                Des fosses communes secrÃ¨tes ont Ã©tÃ© creusÃ©es dans la forÃªt de Fontainebleau pour enterrer les victimes du vaccin. Les familles reÃ§oivent de faux certificats de dÃ©cÃ¨s mentionnant d'autres causes. Un lanceur d'alerte de l'administration pÃ©nitentiaire confirme que des dÃ©tenus sont utilisÃ©s pour creuser les tombes la nuit.""",
            },
            # German - Economic conspiracy
            {
                "language": "German",
                "content": """SKANDAL: Olaf Scholz verkauft Deutschland heimlich an chinesische Investoren
