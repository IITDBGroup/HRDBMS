package com.exascale.huffman;

import java.io.BufferedReader;
import java.io.FileReader;

public class BuildDecode
{
	private static final int NUM_SYM = 668;
	private static int[][] codeExtended = new int[256][256];
	private static int[][] codeExtended2 = new int[NUM_SYM - 256][256];

	public static void main(String[] args)
	{
		try
		{
			codeExtended['t']['h'] = 256;
			codeExtended['T']['H'] = 257;
			codeExtended['t']['H'] = 258;
			codeExtended['T']['h'] = 259;
			codeExtended['h']['e'] = 260;
			codeExtended['H']['E'] = 261;
			codeExtended['h']['E'] = 262;
			codeExtended['H']['e'] = 263;
			codeExtended['i']['n'] = 272;
			codeExtended['I']['N'] = 273;
			codeExtended['I']['n'] = 274;
			codeExtended['i']['N'] = 275;
			codeExtended['e']['r'] = 276;
			codeExtended['E']['R'] = 277;
			codeExtended['E']['r'] = 278;
			codeExtended['e']['R'] = 279;
			codeExtended['a']['n'] = 280;
			codeExtended['A']['N'] = 281;
			codeExtended['A']['n'] = 282;
			codeExtended['a']['N'] = 283;
			codeExtended['r']['e'] = 284;
			codeExtended['R']['E'] = 285;
			codeExtended['R']['e'] = 286;
			codeExtended['r']['E'] = 287;
			codeExtended['o']['n'] = 288;
			codeExtended['O']['N'] = 289;
			codeExtended['O']['n'] = 290;
			codeExtended['o']['N'] = 291;
			codeExtended['a']['t'] = 292;
			codeExtended['A']['T'] = 293;
			codeExtended['A']['t'] = 294;
			codeExtended['a']['T'] = 295;
			codeExtended['e']['n'] = 296;
			codeExtended['E']['N'] = 297;
			codeExtended['E']['n'] = 298;
			codeExtended['e']['N'] = 299;
			codeExtended['n']['d'] = 300;
			codeExtended['N']['D'] = 301;
			codeExtended['N']['d'] = 302;
			codeExtended['n']['D'] = 303;
			codeExtended['t']['i'] = 304;
			codeExtended['T']['I'] = 305;
			codeExtended['T']['i'] = 306;
			codeExtended['t']['I'] = 307;
			codeExtended['e']['s'] = 308;
			codeExtended['E']['S'] = 309;
			codeExtended['E']['s'] = 310;
			codeExtended['e']['S'] = 311;
			codeExtended['o']['r'] = 312;
			codeExtended['O']['R'] = 313;
			codeExtended['O']['r'] = 314;
			codeExtended['o']['R'] = 315;
			codeExtended['t']['e'] = 316;
			codeExtended['T']['E'] = 317;
			codeExtended['T']['e'] = 318;
			codeExtended['t']['E'] = 319;
			codeExtended['o']['f'] = 320;
			codeExtended['O']['F'] = 321;
			codeExtended['O']['f'] = 322;
			codeExtended['o']['F'] = 323;
			codeExtended['e']['d'] = 324;
			codeExtended['E']['D'] = 325;
			codeExtended['E']['d'] = 326;
			codeExtended['e']['D'] = 327;
			codeExtended['i']['s'] = 328;
			codeExtended['I']['S'] = 329;
			codeExtended['I']['s'] = 330;
			codeExtended['i']['S'] = 331;
			codeExtended['i']['t'] = 332;
			codeExtended['I']['T'] = 333;
			codeExtended['I']['t'] = 334;
			codeExtended['i']['T'] = 335;
			codeExtended['a']['l'] = 336;
			codeExtended['A']['L'] = 337;
			codeExtended['A']['l'] = 338;
			codeExtended['a']['L'] = 339;
			codeExtended['a']['r'] = 340;
			codeExtended['A']['R'] = 341;
			codeExtended['A']['r'] = 342;
			codeExtended['a']['R'] = 343;
			codeExtended['s']['t'] = 344;
			codeExtended['S']['T'] = 345;
			codeExtended['S']['t'] = 346;
			codeExtended['s']['T'] = 347;
			codeExtended['t']['o'] = 348;
			codeExtended['T']['O'] = 349;
			codeExtended['T']['o'] = 350;
			codeExtended['t']['O'] = 351;
			codeExtended['n']['t'] = 352;
			codeExtended['N']['T'] = 353;
			codeExtended['N']['t'] = 354;
			codeExtended['n']['T'] = 355;
			codeExtended['n']['g'] = 356;
			codeExtended['N']['G'] = 357;
			codeExtended['N']['g'] = 358;
			codeExtended['n']['G'] = 359;
			codeExtended['s']['e'] = 368;
			codeExtended['S']['E'] = 369;
			codeExtended['S']['e'] = 370;
			codeExtended['s']['E'] = 371;
			codeExtended['h']['a'] = 372;
			codeExtended['H']['A'] = 373;
			codeExtended['H']['a'] = 374;
			codeExtended['h']['A'] = 375;
			codeExtended['a']['s'] = 376;
			codeExtended['A']['S'] = 377;
			codeExtended['A']['s'] = 378;
			codeExtended['a']['S'] = 379;
			codeExtended['o']['u'] = 380;
			codeExtended['O']['U'] = 381;
			codeExtended['O']['u'] = 382;
			codeExtended['o']['U'] = 383;
			codeExtended['i']['o'] = 384;
			codeExtended['I']['O'] = 385;
			codeExtended['I']['o'] = 386;
			codeExtended['i']['O'] = 387;
			codeExtended['l']['e'] = 388;
			codeExtended['L']['E'] = 389;
			codeExtended['L']['e'] = 390;
			codeExtended['l']['E'] = 391;
			codeExtended['v']['e'] = 392;
			codeExtended['V']['E'] = 393;
			codeExtended['V']['e'] = 394;
			codeExtended['v']['E'] = 395;
			codeExtended['c']['o'] = 396;
			codeExtended['C']['O'] = 397;
			codeExtended['C']['o'] = 398;
			codeExtended['c']['O'] = 399;
			codeExtended['m']['e'] = 400;
			codeExtended['M']['E'] = 401;
			codeExtended['M']['e'] = 402;
			codeExtended['m']['E'] = 403;
			codeExtended['d']['e'] = 404;
			codeExtended['D']['E'] = 405;
			codeExtended['D']['e'] = 406;
			codeExtended['d']['E'] = 407;
			codeExtended['h']['i'] = 408;
			codeExtended['H']['I'] = 409;
			codeExtended['H']['i'] = 410;
			codeExtended['h']['I'] = 411;
			codeExtended['r']['i'] = 412;
			codeExtended['R']['I'] = 413;
			codeExtended['R']['i'] = 414;
			codeExtended['r']['I'] = 415;
			codeExtended['r']['o'] = 416;
			codeExtended['R']['O'] = 417;
			codeExtended['R']['o'] = 418;
			codeExtended['r']['O'] = 419;
			codeExtended['i']['c'] = 420;
			codeExtended['I']['C'] = 421;
			codeExtended['I']['c'] = 422;
			codeExtended['i']['C'] = 423;
			codeExtended['n']['e'] = 440;
			codeExtended['N']['E'] = 441;
			codeExtended['N']['e'] = 442;
			codeExtended['n']['E'] = 443;
			codeExtended['e']['a'] = 444;
			codeExtended['E']['A'] = 445;
			codeExtended['E']['a'] = 446;
			codeExtended['e']['A'] = 447;
			codeExtended['r']['a'] = 448;
			codeExtended['R']['A'] = 449;
			codeExtended['R']['a'] = 450;
			codeExtended['r']['A'] = 451;
			codeExtended['c']['e'] = 452;
			codeExtended['C']['E'] = 453;
			codeExtended['C']['e'] = 454;
			codeExtended['c']['E'] = 455;
			codeExtended['l']['i'] = 456;
			codeExtended['L']['I'] = 457;
			codeExtended['L']['i'] = 458;
			codeExtended['l']['I'] = 459;
			codeExtended['c']['h'] = 460;
			codeExtended['C']['H'] = 461;
			codeExtended['C']['h'] = 462;
			codeExtended['c']['H'] = 463;
			codeExtended['l']['l'] = 464;
			codeExtended['L']['L'] = 465;
			codeExtended['L']['l'] = 466;
			codeExtended['l']['L'] = 467;
			codeExtended['b']['e'] = 468;
			codeExtended['B']['E'] = 469;
			codeExtended['B']['e'] = 470;
			codeExtended['b']['E'] = 471;
			codeExtended['m']['a'] = 472;
			codeExtended['M']['A'] = 473;
			codeExtended['M']['a'] = 474;
			codeExtended['m']['A'] = 475;
			codeExtended['s']['i'] = 476;
			codeExtended['S']['I'] = 477;
			codeExtended['S']['i'] = 478;
			codeExtended['s']['I'] = 479;
			codeExtended['o']['m'] = 480;
			codeExtended['O']['M'] = 481;
			codeExtended['O']['m'] = 482;
			codeExtended['o']['M'] = 483;
			codeExtended['u']['r'] = 484;
			codeExtended['U']['R'] = 485;
			codeExtended['U']['r'] = 486;
			codeExtended['u']['R'] = 487;
			codeExtended['c']['a'] = 488;
			codeExtended['C']['A'] = 489;
			codeExtended['C']['a'] = 490;
			codeExtended['c']['A'] = 491;
			codeExtended['e']['l'] = 492;
			codeExtended['E']['L'] = 493;
			codeExtended['E']['l'] = 494;
			codeExtended['e']['L'] = 495;
			codeExtended['t']['a'] = 496;
			codeExtended['T']['A'] = 497;
			codeExtended['T']['a'] = 498;
			codeExtended['t']['A'] = 499;
			codeExtended['l']['a'] = 500;
			codeExtended['L']['A'] = 501;
			codeExtended['L']['a'] = 502;
			codeExtended['l']['A'] = 503;
			codeExtended['n']['s'] = 504;
			codeExtended['N']['S'] = 505;
			codeExtended['N']['s'] = 506;
			codeExtended['n']['S'] = 507;
			codeExtended['d']['i'] = 508;
			codeExtended['D']['I'] = 509;
			codeExtended['D']['i'] = 510;
			codeExtended['d']['I'] = 511;
			codeExtended['f']['o'] = 512;
			codeExtended['F']['O'] = 513;
			codeExtended['F']['o'] = 514;
			codeExtended['f']['O'] = 515;
			codeExtended['h']['o'] = 516;
			codeExtended['H']['O'] = 517;
			codeExtended['H']['o'] = 518;
			codeExtended['h']['O'] = 519;
			codeExtended['p']['e'] = 520;
			codeExtended['P']['E'] = 521;
			codeExtended['P']['e'] = 522;
			codeExtended['p']['E'] = 523;
			codeExtended['e']['c'] = 524;
			codeExtended['E']['C'] = 525;
			codeExtended['E']['c'] = 526;
			codeExtended['e']['C'] = 527;
			codeExtended['p']['r'] = 528;
			codeExtended['P']['R'] = 529;
			codeExtended['P']['r'] = 530;
			codeExtended['p']['R'] = 531;
			codeExtended['n']['o'] = 532;
			codeExtended['N']['O'] = 533;
			codeExtended['N']['o'] = 534;
			codeExtended['n']['O'] = 535;
			codeExtended['c']['t'] = 536;
			codeExtended['C']['T'] = 537;
			codeExtended['C']['t'] = 538;
			codeExtended['c']['T'] = 539;
			codeExtended['u']['s'] = 540;
			codeExtended['U']['S'] = 541;
			codeExtended['U']['s'] = 542;
			codeExtended['u']['S'] = 543;
			codeExtended['a']['c'] = 544;
			codeExtended['A']['C'] = 545;
			codeExtended['A']['c'] = 546;
			codeExtended['a']['C'] = 547;
			codeExtended['o']['t'] = 548;
			codeExtended['O']['T'] = 549;
			codeExtended['O']['t'] = 550;
			codeExtended['o']['T'] = 551;
			codeExtended['i']['l'] = 552;
			codeExtended['I']['L'] = 553;
			codeExtended['I']['l'] = 554;
			codeExtended['i']['L'] = 555;
			codeExtended['t']['r'] = 556;
			codeExtended['T']['R'] = 557;
			codeExtended['T']['r'] = 558;
			codeExtended['t']['R'] = 559;
			codeExtended['l']['y'] = 560;
			codeExtended['L']['Y'] = 561;
			codeExtended['L']['y'] = 562;
			codeExtended['l']['Y'] = 563;
			codeExtended['n']['c'] = 564;
			codeExtended['N']['C'] = 565;
			codeExtended['N']['c'] = 566;
			codeExtended['n']['C'] = 567;
			codeExtended['e']['t'] = 568;
			codeExtended['E']['T'] = 569;
			codeExtended['E']['t'] = 570;
			codeExtended['e']['T'] = 571;
			codeExtended['u']['t'] = 572;
			codeExtended['U']['T'] = 573;
			codeExtended['U']['t'] = 574;
			codeExtended['u']['T'] = 575;
			codeExtended['s']['s'] = 576;
			codeExtended['S']['S'] = 577;
			codeExtended['S']['s'] = 578;
			codeExtended['s']['S'] = 579;
			codeExtended['s']['o'] = 580;
			codeExtended['S']['O'] = 581;
			codeExtended['S']['o'] = 582;
			codeExtended['s']['O'] = 583;
			codeExtended['r']['s'] = 584;
			codeExtended['R']['S'] = 585;
			codeExtended['R']['s'] = 586;
			codeExtended['r']['S'] = 587;
			codeExtended['u']['n'] = 588;
			codeExtended['U']['N'] = 589;
			codeExtended['U']['n'] = 590;
			codeExtended['u']['N'] = 591;
			codeExtended['l']['o'] = 592;
			codeExtended['L']['O'] = 593;
			codeExtended['L']['o'] = 594;
			codeExtended['l']['O'] = 595;
			codeExtended['w']['a'] = 596;
			codeExtended['W']['A'] = 597;
			codeExtended['W']['a'] = 598;
			codeExtended['w']['A'] = 599;
			codeExtended['g']['e'] = 600;
			codeExtended['G']['E'] = 601;
			codeExtended['G']['e'] = 602;
			codeExtended['g']['E'] = 603;
			codeExtended['i']['e'] = 604;
			codeExtended['I']['E'] = 605;
			codeExtended['I']['e'] = 606;
			codeExtended['i']['E'] = 607;
			codeExtended['w']['h'] = 608;
			codeExtended['W']['H'] = 609;
			codeExtended['W']['h'] = 610;
			codeExtended['w']['H'] = 611;

			codeExtended2[1]['E'] = 264;
			codeExtended2[0]['e'] = 265;
			codeExtended2[3]['e'] = 266;
			codeExtended2[2]['e'] = 267;
			codeExtended2[0]['E'] = 268;
			codeExtended2[1]['e'] = 269;
			codeExtended2[2]['E'] = 270;
			codeExtended2[3]['E'] = 271;
			codeExtended2[24]['d'] = 360;
			codeExtended2[25]['D'] = 361;
			codeExtended2[26]['d'] = 362;
			codeExtended2[27]['d'] = 363;
			codeExtended2[24]['D'] = 364;
			codeExtended2[25]['d'] = 365;
			codeExtended2[27]['D'] = 366;
			codeExtended2[26]['D'] = 367;
			codeExtended2[16]['g'] = 424;
			codeExtended2[17]['G'] = 425;
			codeExtended2[18]['g'] = 426;
			codeExtended2[19]['g'] = 427;
			codeExtended2[16]['G'] = 428;
			codeExtended2[17]['g'] = 429;
			codeExtended2[19]['G'] = 430;
			codeExtended2[18]['G'] = 431;
			codeExtended2[128]['n'] = 432;
			codeExtended2[129]['N'] = 433;
			codeExtended2[130]['n'] = 434;
			codeExtended2[131]['n'] = 435;
			codeExtended2[128]['N'] = 436;
			codeExtended2[129]['n'] = 437;
			codeExtended2[131]['N'] = 438;
			codeExtended2[130]['N'] = 439;
			codeExtended2[48]['o'] = 612;
			codeExtended2[49]['O'] = 613;
			codeExtended2[50]['o'] = 614;
			codeExtended2[51]['o'] = 615;
			codeExtended2[48]['O'] = 616;
			codeExtended2[49]['o'] = 617;
			codeExtended2[50]['O'] = 618;
			codeExtended2[51]['O'] = 619;
			codeExtended2[40]['t'] = 620;
			codeExtended2[41]['T'] = 621;
			codeExtended2[42]['t'] = 622;
			codeExtended2[43]['t'] = 623;
			codeExtended2[40]['T'] = 634;
			codeExtended2[41]['t'] = 625;
			codeExtended2[42]['T'] = 626;
			codeExtended2[43]['T'] = 627;
			codeExtended2[36]['i'] = 628;
			codeExtended2[37]['I'] = 629;
			codeExtended2[38]['i'] = 630;
			codeExtended2[39]['i'] = 631;
			codeExtended2[36]['I'] = 632;
			codeExtended2[37]['i'] = 633;
			codeExtended2[38]['I'] = 634;
			codeExtended2[39]['I'] = 635;
			codeExtended2[256]['r'] = 636;
			codeExtended2[257]['R'] = 637;
			codeExtended2[258]['r'] = 638;
			codeExtended2[259]['r'] = 639;
			codeExtended2[256]['R'] = 640;
			codeExtended2[257]['r'] = 641;
			codeExtended2[258]['R'] = 642;
			codeExtended2[259]['R'] = 643;
			codeExtended2[4]['r'] = 644;
			codeExtended2[5]['R'] = 645;
			codeExtended2[7]['r'] = 646;
			codeExtended2[6]['r'] = 647;
			codeExtended2[4]['R'] = 648;
			codeExtended2[5]['r'] = 649;
			codeExtended2[7]['R'] = 650;
			codeExtended2[6]['R'] = 651;

			BufferedReader in = new BufferedReader(new FileReader("c:\\extended.txt"));
			String line = in.readLine();
			while (line != null)
			{
				int index = line.indexOf("freq.put(");
				int index2 = line.indexOf(",", index);
				int value = Integer.parseInt(line.substring(index + 9, index2).trim());
				index = line.indexOf("//");
				line = line.substring(index + 2).trim();
				if (line.length() == 4)
				{
					int code1 = codeExtended[line.charAt(0)][line.charAt(1)] - 256;
					int code2 = codeExtended2[code1][line.charAt(2)] - 256;
					String out = "codeExtended3[" + code2 + "]['" + line.charAt(3) + "'] = " + value + ";";
					System.out.println(out);
				}

				line = in.readLine();
			}

			/*
			 * BufferedReader in = new BufferedReader(new
			 * FileReader("c:\\extended.txt")); String line = in.readLine(); int
			 * i = 0; String out = ""; while (line != null) { int index =
			 * line.indexOf("//"); line = line.substring(index+2).trim(); int j
			 * = 0; out += "{"; while (j < line.length()) { char c =
			 * line.charAt(j); out += ("'" + c + "'"); if (j < line.length() -
			 * 1) { out += ","; }
			 *
			 * j++; } out += "},"; line = in.readLine(); i++;
			 *
			 * if (i == 8) { System.out.println(out); out = ""; i = 0; } }
			 *
			 * System.out.println(out);
			 */
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
