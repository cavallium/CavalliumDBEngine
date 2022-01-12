package it.cavallium.dbengine.lucene.analyzer;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.icu.ICUCollationAttributeFactory;
import org.apache.lucene.analysis.icu.ICUCollationKeyAnalyzer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class LegacyWordAnalyzer extends Analyzer {

	private final boolean icu;
	private final boolean removeStopWords;
	private final boolean stem;

	public LegacyWordAnalyzer(boolean icu, boolean removeStopWords, boolean stem) {
		this.icu = icu;
		this.removeStopWords = removeStopWords;
		this.stem = stem;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		Tokenizer tokenizer;
		if (icu) {
			tokenizer = new StandardTokenizer(new ICUCollationAttributeFactory(Collator.getInstance(ULocale.ROOT)));
		} else {
			tokenizer = new StandardTokenizer();
		}
		TokenStream tokenStream = tokenizer;
		if (stem) {
			tokenStream = new LengthFilter(tokenStream, 1, 120);
		}
		if (!icu) {
			tokenStream = newCommonFilter(tokenStream, stem);
		}
		if (removeStopWords) {
			tokenStream = new EnglishItalianStopFilter(tokenStream);
		}

		return new TokenStreamComponents(tokenizer, tokenStream);
	}

	@Override
	protected TokenStream normalize(String fieldName, TokenStream in) {
		TokenStream tokenStream = in;
		tokenStream = newCommonNormalizer(tokenStream);
		return tokenStream;
	}

	/**
	 *
	 * @param stem Enable stem filters on words.
	 *              Pass false if it will be used with a n-gram filter
	 */
	public static TokenStream newCommonFilter(TokenStream tokenStream, boolean stem) {
		tokenStream = newCommonNormalizer(tokenStream);
		if (stem) {
			tokenStream = new KStemFilter(tokenStream);
			tokenStream = new EnglishPossessiveFilter(tokenStream);
		}
		return tokenStream;
	}

	public static TokenStream newCommonNormalizer(TokenStream tokenStream) {
		tokenStream = new ASCIIFoldingFilter(tokenStream);
		tokenStream = new LowerCaseFilter(tokenStream);
		return tokenStream;
	}

	public class EnglishItalianStopFilter extends StopFilter {

		private static final CharArraySet stopWords;

		private static final Set<String> stopWordsString;

		/**
		 * Constructs a filter which removes words from the input TokenStream that are named in the Set.
		 *
		 * @param in        Input stream
		 * @see #makeStopSet(String...)
		 */
		public EnglishItalianStopFilter(TokenStream in) {
			super(in, stopWords);
		}

		static {
			var englishStopWords = Set.of("a",
					"an",
					"and",
					"are",
					"as",
					"at",
					"be",
					"but",
					"by",
					"for",
					"if",
					"in",
					"into",
					"is",
					"it",
					"no",
					"not",
					"of",
					"on",
					"or",
					"such",
					"that",
					"the",
					"their",
					"then",
					"there",
					"these",
					"they",
					"this",
					"to",
					"was",
					"will",
					"with"
			);
			var oldItalianStopWords = Set.of("a",
					"abbastanza",
					"abbia",
					"abbiamo",
					"abbiano",
					"abbiate",
					"accidenti",
					"ad",
					"adesso",
					"affinché",
					"agl",
					"agli",
					"ahime",
					"ahimè",
					"ai",
					"al",
					"alcuna",
					"alcuni",
					"alcuno",
					"all",
					"alla",
					"alle",
					"allo",
					"allora",
					"altre",
					"altri",
					"altrimenti",
					"altro",
					"altrove",
					"altrui",
					"anche",
					"ancora",
					"anni",
					"anno",
					"ansa",
					"anticipo",
					"assai",
					"attesa",
					"attraverso",
					"avanti",
					"avemmo",
					"avendo",
					"avente",
					"aver",
					"avere",
					"averlo",
					"avesse",
					"avessero",
					"avessi",
					"avessimo",
					"aveste",
					"avesti",
					"avete",
					"aveva",
					"avevamo",
					"avevano",
					"avevate",
					"avevi",
					"avevo",
					"avrai",
					"avranno",
					"avrebbe",
					"avrebbero",
					"avrei",
					"avremmo",
					"avremo",
					"avreste",
					"avresti",
					"avrete",
					"avrà",
					"avrò",
					"avuta",
					"avute",
					"avuti",
					"avuto",
					"basta",
					"ben",
					"bene",
					"benissimo",
					"brava",
					"bravo",
					"buono",
					"c",
					"caso",
					"cento",
					"certa",
					"certe",
					"certi",
					"certo",
					"che",
					"chi",
					"chicchessia",
					"chiunque",
					"ci",
					"ciascuna",
					"ciascuno",
					"cima",
					"cinque",
					"cio",
					"cioe",
					"cioè",
					"circa",
					"citta",
					"città",
					"ciò",
					"co",
					"codesta",
					"codesti",
					"codesto",
					"cogli",
					"coi",
					"col",
					"colei",
					"coll",
					"coloro",
					"colui",
					"come",
					"cominci",
					"comprare",
					"comunque",
					"con",
					"concernente",
					"conclusione",
					"consecutivi",
					"consecutivo",
					"consiglio",
					"contro",
					"cortesia",
					"cos",
					"cosa",
					"cosi",
					"così",
					"cui",
					"d",
					"da",
					"dagl",
					"dagli",
					"dai",
					"dal",
					"dall",
					"dalla",
					"dalle",
					"dallo",
					"dappertutto",
					"davanti",
					"degl",
					"degli",
					"dei",
					"del",
					"dell",
					"della",
					"delle",
					"dello",
					"dentro",
					"detto",
					"deve",
					"devo",
					"di",
					"dice",
					"dietro",
					"dire",
					"dirimpetto",
					"diventa",
					"diventare",
					"diventato",
					"dopo",
					"doppio",
					"dov",
					"dove",
					"dovra",
					"dovrà",
					"dovunque",
					"due",
					"dunque",
					"durante",
					"e",
					"ebbe",
					"ebbero",
					"ebbi",
					"ecc",
					"ecco",
					"ed",
					"effettivamente",
					"egli",
					"ella",
					"entrambi",
					"eppure",
					"era",
					"erano",
					"eravamo",
					"eravate",
					"eri",
					"ero",
					"esempio",
					"esse",
					"essendo",
					"esser",
					"essere",
					"essi",
					"ex",
					"fa",
					"faccia",
					"facciamo",
					"facciano",
					"facciate",
					"faccio",
					"facemmo",
					"facendo",
					"facesse",
					"facessero",
					"facessi",
					"facessimo",
					"faceste",
					"facesti",
					"faceva",
					"facevamo",
					"facevano",
					"facevate",
					"facevi",
					"facevo",
					"fai",
					"fanno",
					"farai",
					"faranno",
					"fare",
					"farebbe",
					"farebbero",
					"farei",
					"faremmo",
					"faremo",
					"fareste",
					"faresti",
					"farete",
					"farà",
					"farò",
					"fatto",
					"favore",
					"fece",
					"fecero",
					"feci",
					"fin",
					"finalmente",
					"finche",
					"fine",
					"fino",
					"forse",
					"forza",
					"fosse",
					"fossero",
					"fossi",
					"fossimo",
					"foste",
					"fosti",
					"fra",
					"frattempo",
					"fu",
					"fui",
					"fummo",
					"fuori",
					"furono",
					"futuro",
					"generale",
					"gente",
					"gia",
					"giacche",
					"giorni",
					"giorno",
					"giu",
					"già",
					"gli",
					"gliela",
					"gliele",
					"glieli",
					"glielo",
					"gliene",
					"grande",
					"grazie",
					"gruppo",
					"ha",
					"haha",
					"hai",
					"hanno",
					"ho",
					"i",
					"ie",
					"ieri",
					"il",
					"improvviso",
					"in",
					"inc",
					"indietro",
					"infatti",
					"inoltre",
					"insieme",
					"intanto",
					"intorno",
					"invece",
					"io",
					"l",
					"la",
					"lasciato",
					"lato",
					"le",
					"lei",
					"li",
					"lo",
					"lontano",
					"loro",
					"lui",
					"lungo",
					"luogo",
					"là",
					"ma",
					"macche",
					"magari",
					"maggior",
					"mai",
					"male",
					"malgrado",
					"malissimo",
					"me",
					"medesimo",
					"mediante",
					"meglio",
					"meno",
					"mentre",
					"mesi",
					"mezzo",
					"mi",
					"mia",
					"mie",
					"miei",
					"mila",
					"miliardi",
					"milioni",
					"minimi",
					"mio",
					"modo",
					"molta",
					"molti",
					"moltissimo",
					"molto",
					"momento",
					"mondo",
					"ne",
					"negl",
					"negli",
					"nei",
					"nel",
					"nell",
					"nella",
					"nelle",
					"nello",
					"nemmeno",
					"neppure",
					"nessun",
					"nessuna",
					"nessuno",
					"niente",
					"no",
					"noi",
					"nome",
					"non",
					"nondimeno",
					"nonostante",
					"nonsia",
					"nostra",
					"nostre",
					"nostri",
					"nostro",
					"novanta",
					"nove",
					"nulla",
					"nuovi",
					"nuovo",
					"o",
					"od",
					"oggi",
					"ogni",
					"ognuna",
					"ognuno",
					"oltre",
					"oppure",
					"ora",
					"ore",
					"osi",
					"ossia",
					"ottanta",
					"otto",
					"paese",
					"parecchi",
					"parecchie",
					"parecchio",
					"parte",
					"partendo",
					"peccato",
					"peggio",
					"per",
					"perche",
					"perchè",
					"perché",
					"percio",
					"perciò",
					"perfino",
					"pero",
					"persino",
					"persone",
					"però",
					"piedi",
					"pieno",
					"piglia",
					"piu",
					"piuttosto",
					"più",
					"po",
					"pochissimo",
					"poco",
					"poi",
					"poiche",
					"possa",
					"possedere",
					"posteriore",
					"posto",
					"potrebbe",
					"preferibilmente",
					"presa",
					"press",
					"prima",
					"primo",
					"principalmente",
					"probabilmente",
					"promesso",
					"proprio",
					"puo",
					"pure",
					"purtroppo",
					"può",
					"qua",
					"qualche",
					"qualcosa",
					"qualcuna",
					"qualcuno",
					"quale",
					"quali",
					"qualunque",
					"quando",
					"quanta",
					"quante",
					"quanti",
					"quanto",
					"quantunque",
					"quarto",
					"quasi",
					"quattro",
					"quel",
					"quella",
					"quelle",
					"quelli",
					"quello",
					"quest",
					"questa",
					"queste",
					"questi",
					"questo",
					"qui",
					"quindi",
					"quinto",
					"realmente",
					"recente",
					"recentemente",
					"registrazione",
					"relativo",
					"riecco",
					"rispetto",
					"salvo",
					"sara",
					"sarai",
					"saranno",
					"sarebbe",
					"sarebbero",
					"sarei",
					"saremmo",
					"saremo",
					"sareste",
					"saresti",
					"sarete",
					"sarà",
					"sarò",
					"scola",
					"scopo",
					"scorso",
					"se",
					"secondo",
					"seguente",
					"seguito",
					"sei",
					"sembra",
					"sembrare",
					"sembrato",
					"sembrava",
					"sembri",
					"sempre",
					"senza",
					"sette",
					"si",
					"sia",
					"siamo",
					"siano",
					"siate",
					"siete",
					"sig",
					"solito",
					"solo",
					"soltanto",
					"sono",
					"sopra",
					"soprattutto",
					"sotto",
					"spesso",
					"sta",
					"stai",
					"stando",
					"stanno",
					"starai",
					"staranno",
					"starebbe",
					"starebbero",
					"starei",
					"staremmo",
					"staremo",
					"stareste",
					"staresti",
					"starete",
					"starà",
					"starò",
					"stata",
					"state",
					"stati",
					"stato",
					"stava",
					"stavamo",
					"stavano",
					"stavate",
					"stavi",
					"stavo",
					"stemmo",
					"stessa",
					"stesse",
					"stessero",
					"stessi",
					"stessimo",
					"stesso",
					"steste",
					"stesti",
					"stette",
					"stettero",
					"stetti",
					"stia",
					"stiamo",
					"stiano",
					"stiate",
					"sto",
					"su",
					"sua",
					"subito",
					"successivamente",
					"successivo",
					"sue",
					"sugl",
					"sugli",
					"sui",
					"sul",
					"sull",
					"sulla",
					"sulle",
					"sullo",
					"suo",
					"suoi",
					"tale",
					"tali",
					"talvolta",
					"tanto",
					"te",
					"tempo",
					"terzo",
					"th",
					"ti",
					"titolo",
					"tra",
					"tranne",
					"tre",
					"trenta",
					"triplo",
					"troppo",
					"trovato",
					"tu",
					"tua",
					"tue",
					"tuo",
					"tuoi",
					"tutta",
					"tuttavia",
					"tutte",
					"tutti",
					"tutto",
					"uguali",
					"ulteriore",
					"ultimo",
					"un",
					"una",
					"uno",
					"uomo",
					"va",
					"vai",
					"vale",
					"vari",
					"varia",
					"varie",
					"vario",
					"verso",
					"vi",
					"vicino",
					"visto",
					"vita",
					"voi",
					"volta",
					"volte",
					"vostra",
					"vostre",
					"vostri",
					"vostro",
					"è");
			var italianStopWords = Set.of("a",
					"abbia",
					"abbiamo",
					"abbiano",
					"abbiate",
					"ad",
					"adesso",
					"agl",
					"agli",
					"ai",
					"al",
					"all",
					"alla",
					"alle",
					"allo",
					"allora",
					"altre",
					"altri",
					"altro",
					"anche",
					"ancora",
					"avemmo",
					"avendo",
					"avere",
					"avesse",
					"avessero",
					"avessi",
					"avessimo",
					"aveste",
					"avesti",
					"avete",
					"aveva",
					"avevamo",
					"avevano",
					"avevate",
					"avevi",
					"avevo",
					"avrai",
					"avranno",
					"avrebbe",
					"avrebbero",
					"avrei",
					"avremmo",
					"avremo",
					"avreste",
					"avresti",
					"avrete",
					"avrà",
					"avrò",
					"avuta",
					"avute",
					"avuti",
					"avuto",
					"c",
					"che",
					"chi",
					"ci",
					"coi",
					"col",
					"come",
					"con",
					"contro",
					"cui",
					"da",
					"dagl",
					"dagli",
					"dai",
					"dal",
					"dall",
					"dalla",
					"dalle",
					"dallo",
					"degl",
					"degli",
					"dei",
					"del",
					"dell",
					"della",
					"delle",
					"dello",
					"dentro",
					"di",
					"dov",
					"dove",
					"e",
					"ebbe",
					"ebbero",
					"ebbi",
					"ecco",
					"ed",
					"era",
					"erano",
					"eravamo",
					"eravate",
					"eri",
					"ero",
					"essendo",
					"faccia",
					"facciamo",
					"facciano",
					"facciate",
					"faccio",
					"facemmo",
					"facendo",
					"facesse",
					"facessero",
					"facessi",
					"facessimo",
					"faceste",
					"facesti",
					"faceva",
					"facevamo",
					"facevano",
					"facevate",
					"facevi",
					"facevo",
					"fai",
					"fanno",
					"farai",
					"faranno",
					"fare",
					"farebbe",
					"farebbero",
					"farei",
					"faremmo",
					"faremo",
					"fareste",
					"faresti",
					"farete",
					"farà",
					"farò",
					"fece",
					"fecero",
					"feci",
					"fino",
					"fosse",
					"fossero",
					"fossi",
					"fossimo",
					"foste",
					"fosti",
					"fra",
					"fu",
					"fui",
					"fummo",
					"furono",
					"giù",
					"gli",
					"ha",
					"hai",
					"hanno",
					"ho",
					"i",
					"il",
					"in",
					"io",
					"l",
					"la",
					"le",
					"lei",
					"li",
					"lo",
					"loro",
					"lui",
					"ma",
					"me",
					"mi",
					"mia",
					"mie",
					"miei",
					"mio",
					"ne",
					"negl",
					"negli",
					"nei",
					"nel",
					"nell",
					"nella",
					"nelle",
					"nello",
					"no",
					"noi",
					"non",
					"nostra",
					"nostre",
					"nostri",
					"nostro",
					"o",
					"per",
					"perché",
					"però",
					"più",
					"pochi",
					"poco",
					"qua",
					"quale",
					"quanta",
					"quante",
					"quanti",
					"quanto",
					"quasi",
					"quella",
					"quelle",
					"quelli",
					"quello",
					"questa",
					"queste",
					"questi",
					"questo",
					"qui",
					"quindi",
					"sarai",
					"saranno",
					"sarebbe",
					"sarebbero",
					"sarei",
					"saremmo",
					"saremo",
					"sareste",
					"saresti",
					"sarete",
					"sarà",
					"sarò",
					"se",
					"sei",
					"senza",
					"si",
					"sia",
					"siamo",
					"siano",
					"siate",
					"siete",
					"sono",
					"sopra",
					"sotto",
					"sta",
					"stai",
					"stando",
					"stanno",
					"starai",
					"staranno",
					"stare",
					"starebbe",
					"starebbero",
					"starei",
					"staremmo",
					"staremo",
					"stareste",
					"staresti",
					"starete",
					"starà",
					"starò",
					"stava",
					"stavamo",
					"stavano",
					"stavate",
					"stavi",
					"stavo",
					"stemmo",
					"stesse",
					"stessero",
					"stessi",
					"stessimo",
					"stesso",
					"steste",
					"stesti",
					"stette",
					"stettero",
					"stetti",
					"stia",
					"stiamo",
					"stiano",
					"stiate",
					"sto",
					"su",
					"sua",
					"sue",
					"sugl",
					"sugli",
					"sui",
					"sul",
					"sull",
					"sulla",
					"sulle",
					"sullo",
					"suo",
					"suoi",
					"te",
					"ti",
					"tra",
					"tu",
					"tua",
					"tue",
					"tuo",
					"tuoi",
					"tutti",
					"tutto",
					"un",
					"una",
					"uno",
					"vai",
					"vi",
					"voi",
					"vostra",
					"vostre",
					"vostri",
					"vostro",
					"è"
			);
			var stopWordsString2 = new HashSet<>(englishStopWords);
			stopWordsString2.addAll(italianStopWords);
			stopWordsString = Collections.unmodifiableSet(stopWordsString2);
			stopWords = CharArraySet.copy(Stream
					.concat(englishStopWords.stream(), oldItalianStopWords.stream())
					.map(String::toCharArray)
					.collect(Collectors.toSet()));
		}

		@SuppressWarnings("unused")
		public static CharArraySet getStopWords() {
			return stopWords;
		}

		public static Set<String> getStopWordsString() {
			return stopWordsString;
		}
	}

}
