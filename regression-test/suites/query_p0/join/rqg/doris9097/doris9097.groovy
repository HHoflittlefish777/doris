// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("doris9097") {
    sql """
    DROP TABLE IF EXISTS `table_200_undef_undef`;
    """
    sql """
    create table table_200_undef_undef (
`pk` int,
`col_bigint_undef_signed_` bigint   ,
`col_bigint_undef_signed_2` bigint   
) engine=olap
distributed by hash(pk) buckets 10
properties(
	'replication_num' = '1');
    """

    sql """
    DROP TABLE IF EXISTS `table_200_undef_undef2`;
    """
    sql """
    create table table_200_undef_undef2 (
`pk` int,
`col_bigint_undef_signed_` bigint   ,
`col_bigint_undef_signed_2` bigint   
) engine=olap
distributed by hash(pk) buckets 10
properties(
	'replication_num' = '1');
    """


    sql """
        insert into table_200_undef_undef values (0,-4165951762050400392,-14),(1,42,-22),(2,null,-31036),(3,55,5122904558131244484),(4,-14840,28281),(5,null,4998879),(6,5760847056946960390,-9011739143110076452),(7,-4639555,-21141),(8,69,null),(9,-33,47),(10,-2050470,-2653),(11,-4395444096438300201,-3096865741370563762),(12,-57,8933),(13,3365621276362527887,-16556),(14,null,8639540759788641439),(15,-2209385869700130743,-6471665),(16,-381175611996409568,6121677622945129007),(17,-9,16),(18,5261214778918955972,96),(19,14558,-3190030),(20,4002210367497857601,null),(21,2437165,-2417492463683254524),(22,2026291529499254125,-30106),(23,null,-83),(24,-7040,-7597554640479267935),(25,3060650817839057930,7511059121559581870),(26,-25088,37),(27,3286825761820654939,null),(28,-1696890,6984780763526232152),(29,null,607433514646944202),(30,6501667047378286768,2484999127969171851),(31,-1851929377371097733,-8944201460241552737),(32,5084060818860982754,-4761807),(33,3052839,-998765803011921378),(34,null,-2422426),(35,3615385,8226094),(36,-1764,553468726038220225),(37,-101,670883),(38,-120,70),(39,59,-3776715),(40,8421394462175681931,4838937589070057812),(41,77,-8994792392196862725),(42,37628358154783384,358664322720405440),(43,5195869005935872176,-10499),(44,null,7094832329480659747),(45,null,-3766135074507305544),(46,null,-19497),(47,-3306469,null),(48,-1760325,-8528460007943599117),(49,-5730441742947563969,51),(50,-35,4904),(51,2266909843852358544,null),(52,-26111,null),(53,5945675,-7461439),(54,23553,4702),(55,-4912359469229728482,8714341737190065819),(56,-5510453229343346156,-5772956446943208518),(57,11380,22147),(58,4844883274403358382,-271231),(59,-6225263610759264362,null),(60,-7032191710347925689,4911949562198616949),(61,7721423,2366848313821501436),(62,null,-2356829323247206674),(63,4913409987497477076,3438657),(64,-6184634268801161880,205319),(65,8575308369769699770,936397305511590189),(66,null,-21036),(67,null,4752785390673666391),(68,6817864,-8170821256325143097),(69,-214665093470046299,-2224094628387595468),(70,9,null),(71,9089379410838309690,7230382),(72,-49,-5237482133941049538),(73,-3172351743764464897,-42),(74,null,-13827),(75,-5649032,3706599682931749944),(76,-2140615177833884381,14643),(77,null,null),(78,750767363278959554,-3795163407934655216),(79,null,null),(80,6975784,6926419441738996233),(81,-9035713900849626698,126),(82,-108,-2538385782765190414),(83,-4549922981680820874,null),(84,16024,4319827),(85,4073160,6945582),(86,-692268,null),(87,-7072,4245304169440330766),(88,-2365942883662944868,5945276832456638628),(89,-2388393067867782794,null),(90,null,-5214488184918655339),(91,null,30283),(92,38,-90),(93,122,-25524),(94,-2367701296362663887,-7862022026564249911),(95,-495064661339257714,-817367),(96,505064392123637243,-6477009766812963182),(97,-966726935248378936,-4212727),(98,20,2710217762115406921),(99,17057,33),(100,-2607623,297646150647682171),(101,-3232274902461632667,-88),(102,null,-17),(103,-7143380563506233557,4735099315894678131),(104,null,-6114629),(105,54,-6040087),(106,5694210904475655638,null),(107,null,-8387506967423869699),(108,-1982987472413558725,-1200509747943337285),(109,4804116496319588906,-5787994070893010124),(110,-3828113,-5847622334778768426),(111,-4830400489803902514,-7145913),(112,null,-6791542),(113,-28676,102),(114,-361992158254727273,4559705686744173283),(115,-4962511944791919861,24),(116,null,6736805),(117,-6640685,714332520320791632),(118,null,665818),(119,6074163,null),(120,-689070,null),(121,25064,-24),(122,null,-78),(123,-24,-549609113203121688),(124,null,867146817841041115),(125,null,18),(126,3285560946807474017,null),(127,22757,-121),(128,10237,-168249),(129,7481297522829648294,null),(130,-6399586,30251),(131,1004467,4913495183669183344),(132,5158677,-125),(133,14277,6796363357274032079),(134,5767025869625213417,null),(135,2118224,26050),(136,1093528676464769316,-28074),(137,1955694,5420832),(138,383669235443209662,null),(139,-62,2946121),(140,-9103267825327065989,-46),(141,null,-2949206507486668335),(142,41,7036366),(143,-27383,-2416),(144,-58,5263065124401825017),(145,93580,18339),(146,null,-2823256),(147,5357061248952387955,31),(148,5832941602789907383,-7790563706290535640),(149,-3509356,-3582753873899681689),(150,2631959234629626497,7698857985126772079),(151,-7241526962894484810,-22),(152,1120676,32766),(153,null,121),(154,-32573,-73),(155,null,-267140),(156,3956401,-3175935028278370307),(157,null,-2640860357207331704),(158,-829791381322143745,-23),(159,9198943545490679406,null),(160,-9,4663366),(161,-5324668,null),(162,-7894,-25386),(163,-2987157224263042376,5935697978930437786),(164,null,null),(165,15624,3079442770836003020),(166,2844590,null),(167,null,8174989),(168,null,-5234876096344461099),(169,-808535820293485985,null),(170,4618027729343988180,3047),(171,-8850259923204901454,-3479334715353462914),(172,-7948216698165016748,7134764104706561197),(173,7983,null),(174,7936371815602286177,9094192116522804213),(175,-19186,115),(176,-94,-15),(177,8118110311274911792,4149688),(178,6082409991708653871,24),(179,175271238959455287,10855),(180,-8711326855478897833,6371602194660725537),(181,5763795460485481412,null),(182,1906052682016060224,-8307328),(183,12268,null),(184,null,3046273),(185,6916090673012065327,-6588939),(186,null,null),(187,-87,8041856941272546055),(188,null,-10281),(189,39,null),(190,27,23377),(191,8627434086799523779,4),(192,-23538,-1460261672930855623),(193,-29,-2127548144750619471),(194,5163552029475087908,-7734454),(195,122,-2678129615356484135),(196,-71,null),(197,null,16978),(198,-27868,8853),(199,13182,-428134739055343020);
    """

    sql """
        
insert into table_200_undef_undef2 values (0,-83475308582731078,4757385),(1,null,5656162275530927275),(2,-4774487848194004407,null),(3,-24673,-6121245078848807710),(4,-4208481,null),(5,6661054439183727345,null),(6,69,-21866),(7,32486,-4563537065551755303),(8,792478478220715600,85),(9,-948309,null),(10,-2660395,303018125117652688),(11,-377496001318248765,17),(12,6682035843545314916,null),(13,-3300872733660321156,19151),(14,8183214633031218826,-6204508354834430039),(15,-27,-3806859793473971027),(16,-3894809580722583190,null),(17,-6538648797808618861,4764933446351483666),(18,-8780239937778261932,-538729092754807513),(19,3536882494626229928,25503),(20,6350309073471886913,-25524),(21,-4116673,-84),(22,3244801137281014439,-4167738067629218759),(23,-7873073460269246550,4745179507115198714),(24,7846,85),(25,-4982953,-5110336009008971386),(26,-5681860,1172838),(27,-2819410375319461119,-8370768),(28,null,-125),(29,45,8200641591555842135),(30,19691,-3529245),(31,24940,6087757),(32,8010709,-58),(33,null,null),(34,-2768,86),(35,null,23306),(36,25682,96),(37,8368313733689024685,-1289055326189811513),(38,54,-100),(39,-5896079370725226278,null),(40,8010954,-7380988144375800727),(41,-6603929591319379128,-7818801527382885938),(42,-7909606,5978962003708099348),(43,20777,-7486085896466661673),(44,null,-4106862),(45,15498,8732461151344618510),(46,null,-23163),(47,-6080225603581159218,7624),(48,-4210477,-22),(49,3622601400737359153,3573032),(50,7405,-24),(51,null,-1271896170035557432),(52,-13,-9094209033132387803),(53,456931,null),(54,9076505354424397795,100),(55,6535,26),(56,7428951067806351882,-1816521),(57,-71,4088573),(58,8824988362339617081,10969),(59,-7386864292933701084,8036421),(60,-5840318576757695972,2006877737624803996),(61,351,-5885217730857418596),(62,-878223,null),(63,8727449850941351674,-4215355),(64,10607,4888781528424256946),(65,8065356283152536901,-107),(66,67,-9189110878398792573),(67,null,6912),(68,-7062061,-91),(69,-7749108039107964555,-405350279810329905),(70,null,-2547944),(71,13,24876),(72,null,-3255082),(73,null,-25715),(74,-22663,-2936578789764032820),(75,-8821338568859706221,-47),(76,7080863,null),(77,-123,-6196927),(78,3629463513719687247,-57),(79,86,null),(80,2435086064121403984,-122),(81,5005184444927139059,108),(82,-7841232,8644545422562154815),(83,2573861,null),(84,-18392,-1399955),(85,-103,-31663),(86,-8555320598166541589,194287),(87,-4892,-111),(88,null,-5033359),(89,9157977834282763838,4518085127086285685),(90,15346,1335567),(91,15135,2891),(92,-5805858302218717814,-39),(93,-5808910004953850811,null),(94,23176,null),(95,null,-981422644958937334),(96,9173813753625204121,-4355008227046747),(97,null,null),(98,-80,1),(99,61,24967),(100,78,-3818167),(101,441549335833186403,-1223141756886847145),(102,-3111505,null),(103,3916463336265848200,7368577),(104,1081373816338645240,7681792149215482846),(105,9127856023580134465,-8423262785170881092),(106,-945219,-31709),(107,null,-57),(108,2835548,43),(109,-4178064,261981072191633796),(110,-2449691360231967494,-7841087),(111,-127,-1046),(112,null,9324),(113,-8012757952269675796,7816867),(114,-14660,484445312077890868),(115,7860152785668511582,-1529665347266975881),(116,null,-43),(117,3348154929820369730,null),(118,4952312156401302597,-71),(119,8162538405971022122,20519),(120,-64,-1438361889919249203),(121,-21220,-8753),(122,-4331571766088428928,9142476150004085514),(123,3710626015019385855,30659),(124,-3465998351316448593,-21),(125,-7849834936419438085,11008),(126,-90,null),(127,-5202716174347623607,-5713311),(128,6192908823439560265,8208403435501513817),(129,4571506,-8085081),(130,-21148,-22065),(131,-29,63),(132,29,8301545),(133,1939,-12698),(134,4303672889135809241,-15987),(135,null,3144284546752428098),(136,23288,113),(137,-2254,1327370),(138,-2837542,2443258503085327082),(139,6153512,21671),(140,7523582,-1904530212910064205),(141,2199827566264043270,-14230),(142,-3219467993798855068,6271300),(143,12325,77),(144,50,5625971964453110622),(145,-62,6937229510861287071),(146,7761646,1923432207113650397),(147,7286184,null),(148,-4160207251524039436,-5220410029888333771),(149,null,94),(150,null,-2271),(151,-3477927723262728703,2700508235578770145),(152,4341111889488820412,null),(153,-5418854114288664234,-2113631356376583677),(154,-118988437507601024,22117),(155,null,-4481770475682220661),(156,-6772237,null),(157,-5614961954242207203,null),(158,null,9101753392684957291),(159,-6423616039960775191,105),(160,null,4118355),(161,6997614132006879961,null),(162,5280503956616296050,5596318228345562208),(163,null,null),(164,-8758949315990387751,-61),(165,-588596054158176996,-7012796376103027531),(166,-4080695214472575894,-125),(167,-8816114154807471465,-29573),(168,-19,8269829172781562504),(169,-3004876807629209356,451773531773852531),(170,-819393458026738127,null),(171,-1555855863925899523,14808),(172,6008829852407875651,-6356688264931671593),(173,-2688197047526950466,81),(174,6852555480730956744,-116),(175,null,-7425744717992202159),(176,3111459,-40),(177,5976857994442635189,-10),(178,null,7396126665043290109),(179,-1654206292997912460,null),(180,-3419286,26871),(181,3573780,null),(182,null,-4316750416983307655),(183,-58,-3628561),(184,7888,-29321),(185,-99,-120966193990282513),(186,11102,-14320),(187,-8018629567817464414,null),(188,-107,21999),(189,6991175692226133232,-8182222994537385562),(190,-16475,-7084614868942879982),(191,11356,null),(192,6400934,-6677810657671757028),(193,-7428,1728),(194,null,-5171),(195,null,null),(196,10282,5181088349455790202),(197,-93,4189367362898487507),(198,8651852209067563935,null),(199,null,-1545427);
    """

    qt_sql """
SELECT T1.pk AS C1,
	T1.`col_bigint_undef_signed_2` AS C2,
	T1.`col_bigint_undef_signed_` AS C3
FROM table_200_undef_undef AS T1
FULL JOIN table_200_undef_undef2 AS T2 ON T1.`col_bigint_undef_signed_2` != T2.`col_bigint_undef_signed_`
	AND T1.`col_bigint_undef_signed_2` IN (
		SELECT T3.`col_bigint_undef_signed_2`
		FROM table_200_undef_undef2 AS T3
		WHERE T1.`col_bigint_undef_signed_` <> T3.`col_bigint_undef_signed_`
		) order by 1,2,3;
    """
}