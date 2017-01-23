package com.exascale.huffman;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class Huffman
{
	private final static int NUM_SYM = 668;
	// private static HashMap<Integer, Integer> freq = new HashMap<Integer,
	// Integer>();
	// private static HashMap<Integer, HuffmanNode> treeParts = new
	// HashMap<Integer, HuffmanNode>();
	// private static HuffmanNode tree;
	private final static int[] encode = new int[NUM_SYM];
	private final static int[] encodeLength = new int[NUM_SYM];
	private final static int[] masks = { 0, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff, 0x01ff, 0x03ff, 0x07ff, 0x0fff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x01ffff, 0x03ffff, 0x07ffff, 0x0fffff, 0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff };
	private final static Code[] decode3 = new Code[16777216];
	private final static int[][] codeExtended = new int[256][256];
	private final static int[][] codeExtended2 = new int[NUM_SYM - 256][256];
	private final static int[][] codeExtended3 = new int[NUM_SYM - 256][256];
	private static HashMap<Integer, Integer> freq = new HashMap<Integer, Integer>();
	private static HashMap<Integer, HuffmanNode> treeParts = new HashMap<Integer, HuffmanNode>();
	private static HuffmanNode tree;

	private final static byte[][] decodeExtended = { { 't', 'h' }, { 'T', 'H' }, { 't', 'H' }, { 'T', 'h' }, { 'h', 'e' }, { 'H', 'E' }, { 'h', 'E' }, { 'H', 'e' }, { 'T', 'H', 'E' }, { 't', 'h', 'e' }, { 'T', 'h', 'e' }, { 't', 'H', 'e' }, { 't', 'h', 'E' }, { 'T', 'H', 'e' }, { 't', 'H', 'E' }, { 'T', 'h', 'E' }, { 'i', 'n' }, { 'I', 'N' }, { 'I', 'n' }, { 'i', 'N' }, { 'e', 'r' }, { 'E', 'R' }, { 'E', 'r' }, { 'e', 'R' }, { 'a', 'n' }, { 'A', 'N' }, { 'A', 'n' }, { 'a', 'N' }, { 'r', 'e' }, { 'R', 'E' }, { 'R', 'e' }, { 'r', 'E' }, { 'o', 'n' }, { 'O', 'N' }, { 'O', 'n' }, { 'o', 'N' }, { 'a', 't' }, { 'A', 'T' }, { 'A', 't' }, { 'a', 'T' }, { 'e', 'n' }, { 'E', 'N' }, { 'E', 'n' }, { 'e', 'N' }, { 'n', 'd' }, { 'N', 'D' }, { 'N', 'd' }, { 'n', 'D' }, { 't', 'i' }, { 'T', 'I' }, { 'T', 'i' }, { 't', 'I' }, { 'e', 's' }, { 'E', 'S' }, { 'E', 's' }, { 'e', 'S' }, { 'o', 'r' }, { 'O', 'R' }, { 'O', 'r' }, { 'o', 'R' }, { 't', 'e' }, { 'T', 'E' }, { 'T', 'e' }, { 't', 'E' }, { 'o', 'f' }, { 'O', 'F' }, { 'O', 'f' }, { 'o', 'F' }, { 'e', 'd' }, { 'E', 'D' }, { 'E', 'd' }, { 'e', 'D' }, { 'i', 's' }, { 'I', 'S' }, { 'I', 's' }, { 'i', 'S' }, { 'i', 't' }, { 'I', 'T' }, { 'I', 't' }, { 'i', 'T' }, { 'a', 'l' }, { 'A', 'L' }, { 'A', 'l' }, { 'a', 'L' }, { 'a', 'r' }, { 'A', 'R' }, { 'A', 'r' }, { 'a', 'R' }, { 's', 't' }, { 'S', 'T' }, { 'S', 't' }, { 's', 'T' }, { 't', 'o' }, { 'T', 'O' }, { 'T', 'o' }, { 't', 'O' }, { 'n', 't' }, { 'N', 'T' }, { 'N', 't' }, { 'n', 'T' }, { 'n', 'g' }, { 'N', 'G' }, { 'N', 'g' }, { 'n', 'G' }, { 'a', 'n', 'd' }, { 'A', 'N', 'D' }, { 'A', 'n', 'd' }, { 'a', 'N', 'd' }, { 'a', 'n', 'D' }, { 'A', 'N', 'd' }, { 'a', 'N', 'D' }, { 'A', 'n', 'D' }, { 's', 'e' }, { 'S', 'E' }, { 'S', 'e' }, { 's', 'E' }, { 'h', 'a' }, { 'H', 'A' }, { 'H', 'a' }, { 'h', 'A' }, { 'a', 's' }, { 'A', 'S' }, { 'A', 's' }, { 'a', 'S' }, { 'o', 'u' }, { 'O', 'U' }, { 'O', 'u' }, { 'o', 'U' }, { 'i', 'o' }, { 'I', 'O' }, { 'I', 'o' }, { 'i', 'O' }, { 'l', 'e' }, { 'L', 'E' }, { 'L', 'e' }, { 'l', 'E' }, { 'v', 'e' }, { 'V', 'E' }, { 'V', 'e' }, { 'v', 'E' }, { 'c', 'o' }, { 'C', 'O' }, { 'C', 'o' }, { 'c', 'O' }, { 'm', 'e' }, { 'M', 'E' }, { 'M', 'e' }, { 'm', 'E' }, { 'd', 'e' }, { 'D', 'E' }, { 'D', 'e' }, { 'd', 'E' }, { 'h', 'i' }, { 'H', 'I' }, { 'H', 'i' }, { 'h', 'I' }, { 'r', 'i' }, { 'R', 'I' }, { 'R', 'i' }, { 'r', 'I' }, { 'r', 'o' }, { 'R', 'O' }, { 'R', 'o' }, { 'r', 'O' }, { 'i', 'c' }, { 'I', 'C' }, { 'I', 'c' }, { 'i', 'C' }, { 'i', 'n', 'g' }, { 'I', 'N', 'G' }, { 'I', 'n', 'g' }, { 'i', 'N', 'g' }, { 'i', 'n', 'G' }, { 'I', 'N', 'g' }, { 'i', 'N', 'G' }, { 'I', 'n', 'G' }, { 'i', 'o', 'n' }, { 'I', 'O', 'N' }, { 'I', 'o', 'n' }, { 'i', 'O', 'n' }, { 'i', 'o', 'N' }, { 'I', 'O', 'n' }, { 'i', 'O', 'N' }, { 'I', 'o', 'N' }, { 'n', 'e' }, { 'N', 'E' }, { 'N', 'e' }, { 'n', 'E' }, { 'e', 'a' }, { 'E', 'A' }, { 'E', 'a' }, { 'e', 'A' }, { 'r', 'a' }, { 'R', 'A' }, { 'R', 'a' }, { 'r', 'A' }, { 'c', 'e' }, { 'C', 'E' }, { 'C', 'e' }, { 'c', 'E' }, { 'l', 'i' }, { 'L', 'I' }, { 'L', 'i' }, { 'l', 'I' }, { 'c', 'h' }, { 'C', 'H' }, { 'C', 'h' }, { 'c', 'H' }, { 'l', 'l' }, { 'L', 'L' }, { 'L', 'l' }, { 'l', 'L' }, { 'b', 'e' }, { 'B', 'E' }, { 'B', 'e' }, { 'b', 'E' }, { 'm', 'a' }, { 'M', 'A' }, { 'M', 'a' }, { 'm', 'A' }, { 's', 'i' }, { 'S', 'I' }, { 'S', 'i' }, { 's', 'I' }, { 'o', 'm' }, { 'O', 'M' }, { 'O', 'm' }, { 'o', 'M' }, { 'u', 'r' }, { 'U', 'R' }, { 'U', 'r' }, { 'u', 'R' }, { 'c', 'a' }, { 'C', 'A' }, { 'C', 'a' }, { 'c', 'A' }, { 'e', 'l' }, { 'E', 'L' }, { 'E', 'l' }, { 'e', 'L' }, { 't', 'a' }, { 'T', 'A' }, { 'T', 'a' }, { 't', 'A' }, { 'l', 'a' }, { 'L', 'A' }, { 'L', 'a' }, { 'l', 'A' }, { 'n', 's' }, { 'N', 'S' }, { 'N', 's' }, { 'n', 'S' }, { 'd', 'i' }, { 'D', 'I' }, { 'D', 'i' }, { 'd', 'I' }, { 'f', 'o' }, { 'F', 'O' }, { 'F', 'o' }, { 'f', 'O' }, { 'h', 'o' }, { 'H', 'O' }, { 'H', 'o' }, { 'h', 'O' }, { 'p', 'e' }, { 'P', 'E' }, { 'P', 'e' }, { 'p', 'E' }, { 'e', 'c' }, { 'E', 'C' }, { 'E', 'c' }, { 'e', 'C' }, { 'p', 'r' }, { 'P', 'R' }, { 'P', 'r' }, { 'p', 'R' }, { 'n', 'o' }, { 'N', 'O' }, { 'N', 'o' }, { 'n', 'O' }, { 'c', 't' }, { 'C', 'T' }, { 'C', 't' }, { 'c', 'T' }, { 'u', 's' }, { 'U', 'S' }, { 'U', 's' }, { 'u', 'S' }, { 'a', 'c' }, { 'A', 'C' }, { 'A', 'c' }, { 'a', 'C' }, { 'o', 't' }, { 'O', 'T' }, { 'O', 't' }, { 'o', 'T' }, { 'i', 'l' }, { 'I', 'L' }, { 'I', 'l' }, { 'i', 'L' }, { 't', 'r' }, { 'T', 'R' }, { 'T', 'r' }, { 't', 'R' }, { 'l', 'y' }, { 'L', 'Y' }, { 'L', 'y' }, { 'l', 'Y' }, { 'n', 'c' }, { 'N', 'C' }, { 'N', 'c' }, { 'n', 'C' }, { 'e', 't' }, { 'E', 'T' }, { 'E', 't' }, { 'e', 'T' }, { 'u', 't' }, { 'U', 'T' }, { 'U', 't' }, { 'u', 'T' }, { 's', 's' }, { 'S', 'S' }, { 'S', 's' }, { 's', 'S' }, { 's', 'o' }, { 'S', 'O' }, { 'S', 'o' }, { 's', 'O' }, { 'r', 's' }, { 'R', 'S' }, { 'R', 's' }, { 'r', 'S' }, { 'u', 'n' }, { 'U', 'N' }, { 'U', 'n' }, { 'u', 'N' }, { 'l', 'o' }, { 'L', 'O' }, { 'L', 'o' }, { 'l', 'O' }, { 'w', 'a' }, { 'W', 'A' }, { 'W', 'a' }, { 'w', 'A' }, { 'g', 'e' }, { 'G', 'E' }, { 'G', 'e' }, { 'g', 'E' }, { 'i', 'e' }, { 'I', 'E' }, { 'I', 'e' }, { 'i', 'E' }, { 'w', 'h' }, { 'W', 'H' }, { 'W', 'h' }, { 'w', 'H' }, { 't', 'i', 'o' }, { 'T', 'I', 'O' }, { 'T', 'i', 'o' }, { 't', 'I', 'o' }, { 't', 'i', 'O' }, { 'T', 'I', 'o' }, { 'T', 'i', 'O' }, { 't', 'I', 'O' }, { 'e', 'n', 't' }, { 'E', 'N', 'T' }, { 'E', 'n', 't' }, { 'e', 'N', 't' }, { 'e', 'n', 'T' }, { 'E', 'N', 't' }, { 'E', 'n', 'T' }, { 'e', 'N', 'T' }, { 'a', 't', 'i' }, { 'A', 'T', 'I' }, { 'A', 't', 'i' }, { 'a', 'T', 'i' }, { 'a', 't', 'I' }, { 'A', 'T', 'i' }, { 'A', 't', 'I' }, { 'a', 'T', 'I' }, { 'f', 'o', 'r' }, { 'F', 'O', 'R' }, { 'F', 'o', 'r' }, { 'f', 'O', 'r' }, { 'f', 'o', 'R' }, { 'F', 'O', 'r' }, { 'F', 'o', 'R' }, { 'f', 'O', 'R' }, { 'h', 'e', 'r' }, { 'H', 'E', 'R' }, { 'H', 'e', 'r' }, { 'h', 'E', 'r' }, { 'h', 'e', 'R' }, { 'H', 'E', 'r' }, { 'H', 'e', 'R' }, { 'h', 'E', 'R' }, { 't', 'i', 'o', 'n' }, { 'T', 'I', 'O', 'N' }, { 'T', 'i', 'o', 'n' }, { 't', 'I', 'o', 'n' }, { 't', 'i', 'O', 'n' }, { 't', 'i', 'o', 'N' }, { 'T', 'I', 'o', 'n' }, { 'T', 'i', 'O', 'n' }, { 'T', 'i', 'o', 'N' }, { 't', 'I', 'O', 'n' }, { 't', 'I', 'o', 'N' }, { 't', 'i', 'O', 'N' }, { 'T', 'I', 'O', 'n' }, { 'T', 'I', 'o', 'N' }, { 'T', 'i', 'O', 'N' }, { 't', 'I', 'O', 'N' } };

	static
	{
		long start = System.currentTimeMillis();

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
		codeExtended2[40]['T'] = 624;
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

		codeExtended3[356]['n'] = 652;
		codeExtended3[357]['N'] = 653;
		codeExtended3[358]['n'] = 654;
		codeExtended3[359]['n'] = 655;
		codeExtended3[360]['n'] = 656;
		codeExtended3[356]['N'] = 657;
		codeExtended3[361]['n'] = 658;
		codeExtended3[362]['n'] = 659;
		codeExtended3[358]['N'] = 660;
		codeExtended3[363]['n'] = 661;
		codeExtended3[359]['N'] = 662;
		codeExtended3[360]['N'] = 663;
		codeExtended3[357]['n'] = 664;
		codeExtended3[361]['N'] = 665;
		codeExtended3[362]['N'] = 666;
		codeExtended3[363]['N'] = 667;

		// 1 - non ascii
		// 2 - ascii non char
		// 3 - ascii shouldn't be used
		// 4 - ascii normal but unlikely in DB

		freq.put(0, 100000);
		freq.put(1, 2);
		freq.put(2, 2);
		freq.put(3, 2);
		freq.put(4, 2);
		freq.put(5, 2);
		freq.put(6, 2);
		freq.put(7, 2);
		freq.put(8, 2);
		freq.put(9, 3);
		freq.put(10, 2);
		freq.put(11, 2);
		freq.put(12, 2);
		freq.put(13, 2);
		freq.put(14, 2);
		freq.put(15, 2);
		freq.put(16, 2);
		freq.put(17, 2);
		freq.put(18, 2);
		freq.put(19, 2);
		freq.put(20, 2);
		freq.put(21, 2);
		freq.put(22, 2);
		freq.put(23, 2);
		freq.put(24, 2);
		freq.put(25, 2);
		freq.put(26, 2);
		freq.put(27, 2);
		freq.put(28, 2);
		freq.put(29, 2);
		freq.put(30, 2);
		freq.put(31, 2);
		freq.put(32, 407934);
		freq.put(33, 170);
		freq.put(34, 4);
		freq.put(35, 425);
		freq.put(36, 1333);
		freq.put(37, 380);
		freq.put(38, 536);
		freq.put(39, 4);
		freq.put(40, 5176);
		freq.put(41, 5307);
		freq.put(42, 1493);
		freq.put(43, 511);
		freq.put(44, 17546);
		freq.put(45, 32638);
		freq.put(46, 35940);
		freq.put(47, 3681);
		freq.put(48, 50000);
		freq.put(49, 50000);
		freq.put(50, 50000);
		freq.put(51, 50000);
		freq.put(52, 50000);
		freq.put(53, 50000);
		freq.put(54, 50000);
		freq.put(55, 50000);
		freq.put(56, 50000);
		freq.put(57, 50000);
		freq.put(58, 10347);
		freq.put(59, 2884);
		freq.put(60, 2911);
		freq.put(61, 540);
		freq.put(62, 2952);
		freq.put(63, 3503);
		freq.put(64, 173);
		freq.put(65, 130731);
		freq.put(66, 29367);
		freq.put(67, 59494);
		freq.put(68, 67066);
		freq.put(69, 210175);
		freq.put(70, 35981);
		freq.put(71, 41523);
		freq.put(72, 70732);
		freq.put(73, 124119);
		freq.put(74, 6163);
		freq.put(75, 17680);
		freq.put(76, 79926);
		freq.put(77, 47446);
		freq.put(78, 123062);
		freq.put(79, 141497);
		freq.put(80, 43002);
		freq.put(81, 2525);
		freq.put(82, 107187);
		freq.put(83, 113326);
		freq.put(84, 159271);
		freq.put(85, 51835);
		freq.put(86, 22228);
		freq.put(87, 36979);
		freq.put(88, 5450);
		freq.put(89, 27646);
		freq.put(90, 1597);
		freq.put(91, 205);
		freq.put(92, 37);
		freq.put(93, 210);
		freq.put(94, 8);
		freq.put(95, 2755);
		freq.put(96, 4);
		freq.put(97, 130731);
		freq.put(98, 29367);
		freq.put(99, 59494);
		freq.put(100, 67066);
		freq.put(101, 210175);
		freq.put(102, 35981);
		freq.put(103, 41523);
		freq.put(104, 70732);
		freq.put(105, 124119);
		freq.put(106, 6163);
		freq.put(107, 17680);
		freq.put(108, 79926);
		freq.put(109, 47446);
		freq.put(110, 123062);
		freq.put(111, 141497);
		freq.put(112, 43002);
		freq.put(113, 2525);
		freq.put(114, 107187);
		freq.put(115, 113326);
		freq.put(116, 159271);
		freq.put(117, 51835);
		freq.put(118, 22228);
		freq.put(119, 36979);
		freq.put(120, 5450);
		freq.put(121, 27646);
		freq.put(122, 1597);
		freq.put(123, 62);
		freq.put(124, 16);
		freq.put(125, 61);
		freq.put(126, 8);
		freq.put(127, 2);

		int i = 128;
		while (i < 256)
		{
			freq.put(i++, 1);
		}

		freq.put(256, 47342); // th
		freq.put(257, 47342); // TH
		freq.put(258, 47342); // tH
		freq.put(259, 47342); // Th
		freq.put(260, 40933); // he
		freq.put(261, 40933); // HE
		freq.put(262, 40933); // hE
		freq.put(263, 40933); // He
		freq.put(264, 32681); // THE
		freq.put(265, 32681); // the
		freq.put(266, 32681); // The
		freq.put(267, 32681); // tHe
		freq.put(268, 32681); // thE
		freq.put(269, 32681); // THe
		freq.put(270, 32681); // tHE
		freq.put(271, 32681); // ThE
		freq.put(272, 32386); // in
		freq.put(273, 32386); // IN
		freq.put(274, 32386); // In
		freq.put(275, 32386); // iN
		freq.put(276, 27267); // er
		freq.put(277, 27267); // ER
		freq.put(278, 27267); // Er
		freq.put(279, 27267); // eR
		freq.put(280, 26427); // an
		freq.put(281, 26427); // AN
		freq.put(282, 26427); // An
		freq.put(283, 26427); // aN
		freq.put(284, 24686); // re
		freq.put(285, 24686); // RE
		freq.put(286, 24686); // Re
		freq.put(287, 24686); // rE
		freq.put(288, 23404); // on
		freq.put(289, 23404); // ON
		freq.put(290, 23404); // On
		freq.put(291, 23404); // oN
		freq.put(292, 19792); // at
		freq.put(293, 19792); // AT
		freq.put(294, 19792); // At
		freq.put(295, 19792); // aT
		freq.put(296, 19359); // en
		freq.put(297, 19359); // EN
		freq.put(298, 19359); // En
		freq.put(299, 19359); // eN
		freq.put(300, 18002); // nd
		freq.put(301, 18002); // ND
		freq.put(302, 18002); // Nd
		freq.put(303, 18002); // nD
		freq.put(304, 17873); // ti
		freq.put(305, 17873); // TI
		freq.put(306, 17873); // Ti
		freq.put(307, 17873); // tI
		freq.put(308, 17830); // es
		freq.put(309, 17830); // ES
		freq.put(310, 17830); // Es
		freq.put(311, 17830); // eS
		freq.put(312, 16994); // or
		freq.put(313, 16994); // OR
		freq.put(314, 16994); // Or
		freq.put(315, 16994); // oR
		freq.put(316, 16040); // te
		freq.put(317, 16040); // TE
		freq.put(318, 16040); // Te
		freq.put(319, 16040); // tE
		freq.put(320, 15642); // of
		freq.put(321, 15642); // OF
		freq.put(322, 15642); // Of
		freq.put(323, 15642); // oF
		freq.put(324, 15550); // ed
		freq.put(325, 15550); // ED
		freq.put(326, 15550); // Ed
		freq.put(327, 15550); // eD
		freq.put(328, 15022); // is
		freq.put(329, 15022); // IS
		freq.put(330, 15022); // Is
		freq.put(331, 15022); // iS
		freq.put(332, 14953); // it
		freq.put(333, 14953); // IT
		freq.put(334, 14953); // It
		freq.put(335, 14953); // iT
		freq.put(336, 14476); // al
		freq.put(337, 14476); // AL
		freq.put(338, 14476); // Al
		freq.put(339, 14476); // aL
		freq.put(340, 14309); // ar
		freq.put(341, 14309); // AR
		freq.put(342, 14309); // Ar
		freq.put(343, 14309); // aR
		freq.put(344, 14024); // st
		freq.put(345, 14024); // ST
		freq.put(346, 14024); // St
		freq.put(347, 14024); // sT
		freq.put(348, 13862); // to
		freq.put(349, 13862); // TO
		freq.put(350, 13862); // To
		freq.put(351, 13862); // tO
		freq.put(352, 13861); // nt
		freq.put(353, 13861); // NT
		freq.put(354, 13861); // Nt
		freq.put(355, 13861); // nT
		freq.put(356, 12687); // ng
		freq.put(357, 12687); // NG
		freq.put(358, 12687); // Ng
		freq.put(359, 12687); // nG
		freq.put(360, 12496); // and
		freq.put(361, 12496); // AND
		freq.put(362, 12496); // And
		freq.put(363, 12496); // aNd
		freq.put(364, 12496); // anD
		freq.put(365, 12496); // ANd
		freq.put(366, 12496); // aND
		freq.put(367, 12496); // AnD
		freq.put(368, 12408); // se
		freq.put(369, 12408); // SE
		freq.put(370, 12408); // Se
		freq.put(371, 12408); // sE
		freq.put(372, 12324); // ha
		freq.put(373, 12324); // HA
		freq.put(374, 12324); // Ha
		freq.put(375, 12324); // hA
		freq.put(376, 11596); // as
		freq.put(377, 11596); // AS
		freq.put(378, 11596); // As
		freq.put(379, 11596); // aS
		freq.put(380, 11582); // ou
		freq.put(381, 11582); // OU
		freq.put(382, 11582); // Ou
		freq.put(383, 11582); // oU
		freq.put(384, 11115); // io
		freq.put(385, 11115); // IO
		freq.put(386, 11115); // Io
		freq.put(387, 11115); // iO
		freq.put(388, 11039); // le
		freq.put(389, 11039); // LE
		freq.put(390, 11039); // Le
		freq.put(391, 11039); // lE
		freq.put(392, 10986); // ve
		freq.put(393, 10986); // VE
		freq.put(394, 10986); // Ve
		freq.put(395, 10986); // vE
		freq.put(396, 10568); // co
		freq.put(397, 10568); // CO
		freq.put(398, 10568); // Co
		freq.put(399, 10568); // cO
		freq.put(400, 10557); // me
		freq.put(401, 10557); // ME
		freq.put(402, 10557); // Me
		freq.put(403, 10557); // mE
		freq.put(404, 10181); // de
		freq.put(405, 10181); // DE
		freq.put(406, 10181); // De
		freq.put(407, 10181); // dE
		freq.put(408, 10160); // hi
		freq.put(409, 10160); // HI
		freq.put(410, 10160); // Hi
		freq.put(411, 10160); // hI
		freq.put(412, 9686); // ri
		freq.put(413, 9686); // RI
		freq.put(414, 9686); // Ri
		freq.put(415, 9686); // rI
		freq.put(416, 9674); // ro
		freq.put(417, 9674); // RO
		freq.put(418, 9674); // Ro
		freq.put(419, 9674); // rO
		freq.put(420, 9301); // ic
		freq.put(421, 9301); // IC
		freq.put(422, 9301); // Ic
		freq.put(423, 9301); // iC
		freq.put(424, 10051); // ing
		freq.put(425, 10051); // ING
		freq.put(426, 10051); // Ing
		freq.put(427, 10051); // iNg
		freq.put(428, 10051); // inG
		freq.put(429, 10051); // INg
		freq.put(430, 10051); // iNG
		freq.put(431, 10051); // InG
		freq.put(432, 9654); // ion
		freq.put(433, 9654); // ION
		freq.put(434, 9654); // Ion
		freq.put(435, 9654); // iOn
		freq.put(436, 9654); // ioN
		freq.put(437, 9654); // IOn
		freq.put(438, 9654); // iON
		freq.put(439, 9654); // IoN
		freq.put(440, 9208); // ne
		freq.put(441, 9208); // NE
		freq.put(442, 9208); // Ne
		freq.put(443, 9208); // nE
		freq.put(444, 9161); // ea
		freq.put(445, 9161); // EA
		freq.put(446, 9161); // Ea
		freq.put(447, 9161); // eA
		freq.put(448, 9127); // ra
		freq.put(449, 9127); // RA
		freq.put(450, 9127); // Ra
		freq.put(451, 9127); // rA
		freq.put(452, 8672); // ce
		freq.put(453, 8672); // CE
		freq.put(454, 8672); // Ce
		freq.put(455, 8672); // cE
		freq.put(456, 8311); // li
		freq.put(457, 8311); // LI
		freq.put(458, 8311); // Li
		freq.put(459, 8311); // lI
		freq.put(460, 7957); // ch
		freq.put(461, 7957); // CH
		freq.put(462, 7957); // Ch
		freq.put(463, 7957); // cH
		freq.put(464, 7675); // ll
		freq.put(465, 7675); // LL
		freq.put(466, 7675); // Ll
		freq.put(467, 7675); // lL
		freq.put(468, 7671); // be
		freq.put(469, 7671); // BE
		freq.put(470, 7671); // Be
		freq.put(471, 7671); // bE
		freq.put(472, 7525); // ma
		freq.put(473, 7525); // MA
		freq.put(474, 7525); // Ma
		freq.put(475, 7525); // mA
		freq.put(476, 7322); // si
		freq.put(477, 7322); // SI
		freq.put(478, 7322); // Si
		freq.put(479, 7322); // sI
		freq.put(480, 7272); // om
		freq.put(481, 7272); // OM
		freq.put(482, 7272); // Om
		freq.put(483, 7272); // oM
		freq.put(484, 7225); // ur
		freq.put(485, 7225); // UR
		freq.put(486, 7225); // Ur
		freq.put(487, 7225); // uR
		freq.put(488, 7164); // ca
		freq.put(489, 7164); // CA
		freq.put(490, 7164); // Ca
		freq.put(491, 7164); // cA
		freq.put(492, 7059); // el
		freq.put(493, 7059); // EL
		freq.put(494, 7059); // El
		freq.put(495, 7059); // eL
		freq.put(496, 7054); // ta
		freq.put(497, 7054); // TA
		freq.put(498, 7054); // Ta
		freq.put(499, 7054); // tA
		freq.put(500, 7022); // la
		freq.put(501, 7022); // LA
		freq.put(502, 7022); // La
		freq.put(503, 7022); // lA
		freq.put(504, 6775); // ns
		freq.put(505, 6775); // NS
		freq.put(506, 6775); // Ns
		freq.put(507, 6775); // nS
		freq.put(508, 6562); // di
		freq.put(509, 6562); // DI
		freq.put(510, 6562); // Di
		freq.put(511, 6562); // dI
		freq.put(512, 6493); // fo
		freq.put(513, 6493); // FO
		freq.put(514, 6493); // Fo
		freq.put(515, 6493); // fO
		freq.put(516, 6455); // ho
		freq.put(517, 6455); // HO
		freq.put(518, 6455); // Ho
		freq.put(519, 6455); // hO
		freq.put(520, 6363); // pe
		freq.put(521, 6363); // PE Alex wrote "pe" and other lines in this
		// area while saying pee-pee
		freq.put(522, 6363); // Pe
		freq.put(523, 6363); // pE
		freq.put(524, 6353); // ec
		freq.put(525, 6353); // EC
		freq.put(526, 6353); // Ec
		freq.put(527, 6353); // eC
		freq.put(528, 6316); // pr
		freq.put(529, 6316); // PR
		freq.put(530, 6316); // Pr
		freq.put(531, 6316); // pR
		freq.put(532, 6184); // no
		freq.put(533, 6184); // NO
		freq.put(534, 6184); // No
		freq.put(535, 6184); // nO
		freq.put(536, 6136); // ct
		freq.put(537, 6136); // CT
		freq.put(538, 6136); // Ct
		freq.put(539, 6136); // cT
		freq.put(540, 6047); // us
		freq.put(541, 6047); // US
		freq.put(542, 6047); // Us
		freq.put(543, 6047); // uS
		freq.put(544, 5961); // ac
		freq.put(545, 5961); // AC
		freq.put(546, 5961); // Ac
		freq.put(547, 5961); // aC
		freq.put(548, 5885); // ot
		freq.put(549, 5885); // OT
		freq.put(550, 5885); // Ot
		freq.put(551, 5885); // oT
		freq.put(552, 5744); // il
		freq.put(553, 5744); // IL
		freq.put(554, 5744); // Il
		freq.put(555, 5744); // iL
		freq.put(556, 5668); // tr
		freq.put(557, 5668); // TR
		freq.put(558, 5668); // Tr
		freq.put(559, 5668); // tR
		freq.put(560, 5658); // ly
		freq.put(561, 5658); // LY
		freq.put(562, 5658); // Ly
		freq.put(563, 5658); // lY
		freq.put(564, 5534); // nc
		freq.put(565, 5534); // NC
		freq.put(566, 5534); // Nc
		freq.put(567, 5534); // nC
		freq.put(568, 5492); // et
		freq.put(569, 5492); // ET
		freq.put(570, 5492); // Et
		freq.put(571, 5492); // eT
		freq.put(572, 5393); // ut
		freq.put(573, 5393); // UT
		freq.put(574, 5393); // Ut
		freq.put(575, 5393); // uT
		freq.put(576, 5392); // ss
		freq.put(577, 5392); // SS
		freq.put(578, 5392); // Ss
		freq.put(579, 5392); // sS
		freq.put(580, 5294); // so
		freq.put(581, 5294); // SO
		freq.put(582, 5294); // So
		freq.put(583, 5294); // sO
		freq.put(584, 5278); // rs
		freq.put(585, 5278); // RS
		freq.put(586, 5278); // Rs
		freq.put(587, 5278); // rS
		freq.put(588, 5250); // un
		freq.put(589, 5250); // UN
		freq.put(590, 5250); // Un
		freq.put(591, 5250); // uN
		freq.put(592, 5150); // lo
		freq.put(593, 5150); // LO
		freq.put(594, 5150); // Lo
		freq.put(595, 5150); // lO
		freq.put(596, 5129); // wa
		freq.put(597, 5129); // WA
		freq.put(598, 5129); // Wa
		freq.put(599, 5129); // wA
		freq.put(600, 5127); // ge
		freq.put(601, 5127); // GE
		freq.put(602, 5127); // Ge
		freq.put(603, 5127); // gE
		freq.put(604, 5120); // ie
		freq.put(605, 5120); // IE
		freq.put(606, 5120); // Ie
		freq.put(607, 5120); // iE
		freq.put(608, 5042); // wh
		freq.put(609, 5042); // WH
		freq.put(610, 5042); // Wh
		freq.put(611, 5042); // wH
		freq.put(612, 7941); // tio
		freq.put(613, 7941); // TIO
		freq.put(614, 7941); // Tio
		freq.put(615, 7941); // tIo
		freq.put(616, 7941); // tiO
		freq.put(617, 7941); // TIo
		freq.put(618, 7941); // TiO
		freq.put(619, 7941); // tIO
		freq.put(620, 7324); // ent
		freq.put(621, 7324); // ENT
		freq.put(622, 7324); // Ent
		freq.put(623, 7324); // eNt
		freq.put(624, 7324); // enT
		freq.put(625, 7324); // ENt
		freq.put(626, 7324); // EnT
		freq.put(627, 7324); // eNT
		freq.put(628, 5560); // ati
		freq.put(629, 5560); // ATI
		freq.put(630, 5560); // Ati
		freq.put(631, 5560); // aTi
		freq.put(632, 5560); // atI
		freq.put(633, 5560); // ATi
		freq.put(634, 5560); // AtI
		freq.put(635, 5560); // aTI
		freq.put(636, 5359); // for
		freq.put(637, 5359); // FOR
		freq.put(638, 5359); // For
		freq.put(639, 5359); // fOr
		freq.put(640, 5359); // foR
		freq.put(641, 5359); // FOr
		freq.put(642, 5359); // FoR
		freq.put(643, 5359); // fOR
		freq.put(644, 5156); // her
		freq.put(645, 5156); // HER
		freq.put(646, 5156); // Her
		freq.put(647, 5156); // hEr
		freq.put(648, 5156); // heR
		freq.put(649, 5156); // HEr
		freq.put(650, 5156); // HeR
		freq.put(651, 5156); // hER
		freq.put(652, 7868); // tion
		freq.put(653, 7868); // TION
		freq.put(654, 7868); // Tion
		freq.put(655, 7868); // tIon
		freq.put(656, 7868); // tiOn
		freq.put(657, 7868); // tioN
		freq.put(658, 7868); // TIon
		freq.put(659, 7868); // TiOn
		freq.put(660, 7868); // TioN
		freq.put(661, 7868); // tIOn
		freq.put(662, 7868); // tIoN
		freq.put(663, 7868); // tiON
		freq.put(664, 7868); // TIOn
		freq.put(665, 7868); // TIoN
		freq.put(666, 7868); // TiON
		freq.put(667, 7868); // tION

		buildTree();
		for (int key : freq.keySet())
		{
			tree = treeParts.get(key);
		}

		TreeMap<Integer, String> codes = new TreeMap<Integer, String>();
		traverse(tree, codes, "");
		for (Entry entry : codes.entrySet())
		{
			encodeLength[(Integer)entry.getKey()] = ((String)entry.getValue()).length();
			encode[(Integer)entry.getKey()] = Integer.parseInt((String)entry.getValue(), 2);
		}

		freq.clear();
		treeParts.clear();
		buildDecode3();
		tree = null;

		try
		{
			writeDataFile();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		try
		{
			readDataFile();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		long end = System.currentTimeMillis();
		System.out.println("Init took " + (((end - start) * 1.0) / 1000.0) + "s");
	}

	public static void main(String[] args)
	{

		try
		{
			long start = System.currentTimeMillis();
			BufferedReader in = new BufferedReader(new FileReader("c:\\bible.txt"));
			int u = 0;
			int c = 0;
			String line = in.readLine();
			long lines = 0;
			while (line != null)
			{
				lines++;
				if (line.trim().length() >= 10)
				{
					line = line.trim();
					u += line.length();
					byte[] bytes = line.getBytes("UTF-8");
					byte[] compressed = new byte[bytes.length * 3];
					int clen = compress(bytes, bytes.length, compressed);
					c += clen;
					byte[] decompressed = new byte[clen << 1];
					int dlen = decompress(compressed, clen, decompressed);
					byte[] target = Arrays.copyOf(decompressed, dlen);
					String line2 = new String(target, "UTF-8");
					if (!line.equals(line2))
					{
						System.out.println("Lines don't match!");
						System.out.println(line);
						System.out.println(line2);
					}
				}

				line = in.readLine();
			}

			long end = System.currentTimeMillis();
			in.close();
			System.out.println("Average compression ratio: " + (100.0 - (c * 100.0 / u)) + "%");
			System.out.println("Test took " + ((end - start) / 1000.0) + "s");
			System.out.println("Average line was " + (u * 1.0 / lines) + " characters");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	private static void buildDecode3()
	{
		int i = 0;
		while (i < 16777216)
		{
			Code code = new Code();
			int length = 0;
			int temp = 0;
			ArrayList<Byte> bytes = new ArrayList<Byte>();
			int x = 0x800000;
			HuffmanNode node = tree;
			while (x != 0)
			{
				int z = i & x;
				if (z == 0)
				{
					node = node.left;
				}
				else
				{
					node = node.right;
				}

				temp++;

				if (node.left == null && node.right == null)
				{
					length += temp;
					temp = 0;
					if (node.bytecode < 256)
					{
						bytes.add((byte)node.bytecode);
					}
					else
					{
						byte[] bytes2 = decodeExtended[node.bytecode - 256];
						for (byte b : bytes2)
						{
							bytes.add(b);
						}
					}
					node = tree;
				}

				x = (x >>> 1);
			}

			byte[] array = new byte[bytes.size()];
			int j = 0;
			for (byte b : bytes)
			{
				array[j++] = b;
			}
			code.bytes = array;
			code.used = (short)length;
			decode3[i] = code;

			i++;
		}
	}

	private static void buildTree()
	{
		int i = 0;
		HuffmanNode left = null;
		HuffmanNode right = null;
		for (Entry<Integer, Integer> entry : entriesSortedByValues(freq))
		{
			if (i == 0)
			{
				left = new HuffmanNode(entry.getKey(), entry.getValue());
				i++;
			}
			else
			{
				right = new HuffmanNode(entry.getKey(), entry.getValue());
				break;
			}
		}

		HuffmanNode parent = new HuffmanNode();
		parent.addLeft(left);
		parent.addRight(right);
		freq.remove(left.bytecode);
		freq.remove(right.bytecode);
		parent.bytecode = left.bytecode;
		freq.put(parent.bytecode, parent.freq);
		treeParts.put(parent.bytecode, parent);
		while (freq.size() > 1)
		{
			buildTree2();
		}
	}

	private static void buildTree2()
	{
		int i = 0;
		HuffmanNode left = null;
		HuffmanNode right = null;
		for (Entry<Integer, Integer> entry : entriesSortedByValues(freq))
		{
			if (i == 0)
			{
				left = treeParts.get(entry.getKey());

				if (left == null)
				{
					left = new HuffmanNode(entry.getKey(), entry.getValue());
				}

				i++;
			}
			else
			{
				right = treeParts.get(entry.getKey());

				if (right == null)
				{
					right = new HuffmanNode(entry.getKey(), entry.getValue());
				}

				break;
			}
		}

		HuffmanNode parent = new HuffmanNode();
		parent.addLeft(left);
		parent.addRight(right);
		freq.remove(left.bytecode);
		freq.remove(right.bytecode);
		parent.bytecode = left.bytecode;
		freq.put(parent.bytecode, parent.freq);
		treeParts.put(parent.bytecode, parent);
	}

	/*
	 * private static void buildDecode1() { int i = 0; while (i < 256) { Code
	 * code = new Code(); int length = 0; int temp = 0; ArrayList<Byte> bytes =
	 * new ArrayList<Byte>(); int x = 0x80; HuffmanNode node = tree; while (x !=
	 * 0) { int z = i & x; if (z == 0) { node = node.left; } else { node =
	 * node.right; }
	 *
	 * temp++;
	 *
	 * if (node.left == null && node.right == null) { length += temp; temp = 0;
	 * if (node.bytecode < 256) { bytes.add((byte)node.bytecode); } else {
	 * byte[] bytes2 = decodeExtended[node.bytecode - 256]; for (byte b :
	 * bytes2) { bytes.add(b); } } node = tree; }
	 *
	 * x = (x >>> 1); }
	 *
	 * byte[] array = new byte[bytes.size()]; int j = 0; for (byte b : bytes) {
	 * array[j++] = b; } code.bytes = array; code.used = length; decode1[i] =
	 * code;
	 *
	 * i++; } }
	 *
	 * private static void buildDecode2() { int i = 0; while (i < 65536) { Code
	 * code = new Code(); int length = 0; int temp = 0; ArrayList<Byte> bytes =
	 * new ArrayList<Byte>(); int x = 0x8000; HuffmanNode node = tree; while (x
	 * != 0) { int z = i & x; if (z == 0) { node = node.left; } else { node =
	 * node.right; }
	 *
	 * temp++;
	 *
	 * if (node.left == null && node.right == null) { length += temp; temp = 0;
	 * if (node.bytecode < 256) { bytes.add((byte)node.bytecode); } else {
	 * byte[] bytes2 = decodeExtended[node.bytecode - 256]; for (byte b :
	 * bytes2) { bytes.add(b); } } node = tree; }
	 *
	 * x = (x >>> 1); }
	 *
	 * byte[] array = new byte[bytes.size()]; int j = 0; for (byte b : bytes) {
	 * array[j++] = b; } code.bytes = array; code.used = length; decode2[i] =
	 * code;
	 *
	 * i++; } }
	 *
	 * private static void buildDecode3() { int i = 0; while (i < 16777216) {
	 * Code code = new Code(); int length = 0; int temp = 0; ArrayList<Byte>
	 * bytes = new ArrayList<Byte>(); int x = 0x800000; HuffmanNode node = tree;
	 * while (x != 0) { int z = i & x; if (z == 0) { node = node.left; } else {
	 * node = node.right; }
	 *
	 * temp++;
	 *
	 * if (node.left == null && node.right == null) { length += temp; temp = 0;
	 * if (node.bytecode < 256) { bytes.add((byte)node.bytecode); } else {
	 * byte[] bytes2 = decodeExtended[node.bytecode - 256]; for (byte b :
	 * bytes2) { bytes.add(b); } } node = tree; }
	 *
	 * x = (x >>> 1); }
	 *
	 * byte[] array = new byte[bytes.size()]; int j = 0; for (byte b : bytes) {
	 * array[j++] = b; } code.bytes = array; code.used = length; decode3[i] =
	 * code;
	 *
	 * i++; } }
	 *
	 * private static void traverse(HuffmanNode node, TreeMap<Integer, String>
	 * codes, String str) { if (node.left == null && node.right == null) {
	 * codes.put(node.bytecode, str); } else { if (node.left != null) {
	 * traverse(node.left, codes, str + "0"); }
	 *
	 * if (node.right != null) { traverse(node.right, codes, str + "1"); } } }
	 *
	 * private static void buildTree() { int i = 0; HuffmanNode left = null;
	 * HuffmanNode right = null; for (Entry<Integer, Integer> entry :
	 * entriesSortedByValues(freq)) { if (i == 0) { left = new
	 * HuffmanNode(entry.getKey(), entry.getValue()); i++; } else { right = new
	 * HuffmanNode(entry.getKey(), entry.getValue()); break; } }
	 *
	 * HuffmanNode parent = new HuffmanNode(); parent.addLeft(left);
	 * parent.addRight(right); freq.remove(left.bytecode);
	 * freq.remove(right.bytecode); parent.bytecode = left.bytecode;
	 * freq.put(parent.bytecode, parent.freq); treeParts.put(parent.bytecode,
	 * parent); while (freq.size() > 1) { buildTree2(); } }
	 *
	 * private static void buildTree2() { int i = 0; HuffmanNode left = null;
	 * HuffmanNode right = null; for (Entry<Integer, Integer> entry :
	 * entriesSortedByValues(freq)) { if (i == 0) { left =
	 * treeParts.get(entry.getKey());
	 *
	 * if (left == null) { left = new HuffmanNode(entry.getKey(),
	 * entry.getValue()); }
	 *
	 * i++; } else { right = treeParts.get(entry.getKey());
	 *
	 * if (right == null) { right = new HuffmanNode(entry.getKey(),
	 * entry.getValue()); }
	 *
	 * break; } }
	 *
	 * HuffmanNode parent = new HuffmanNode(); parent.addLeft(left);
	 * parent.addRight(right); freq.remove(left.bytecode);
	 * freq.remove(right.bytecode); parent.bytecode = left.bytecode;
	 * freq.put(parent.bytecode, parent.freq); treeParts.put(parent.bytecode,
	 * parent); }
	 *
	 * private static class HuffmanNode { public int bytecode; public int freq;
	 * public HuffmanNode left; public HuffmanNode right;
	 *
	 * public HuffmanNode(int bytecode, int freq) { this.bytecode = bytecode;
	 * this.freq = freq; }
	 *
	 * public HuffmanNode() { bytecode = -1; freq = 0; }
	 *
	 * public void addLeft(HuffmanNode left) { this.left = left; this.freq +=
	 * left.freq; }
	 *
	 * public void addRight(HuffmanNode right) { this.right = right; this.freq
	 * += right.freq; } }
	 *
	 * static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>>
	 * entriesSortedByValues(Map<K,V> map) { SortedSet<Map.Entry<K,V>>
	 * sortedEntries = new TreeSet<Map.Entry<K,V>>( new
	 * Comparator<Map.Entry<K,V>>() {
	 *
	 * @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) { int
	 * res = e1.getValue().compareTo(e2.getValue()); return res != 0 ? res : 1;
	 * // Special fix to preserve items with equal values } } );
	 * sortedEntries.addAll(map.entrySet()); return sortedEntries; }
	 */

	private static int compress(byte[] in, int inLen, byte[] out)
	{
		int retval = 0;
		int i = 0;
		short x = 32;
		int value = 0;
		int remainder = 0;
		int remLen = 0;
		ByteBuffer bb = ByteBuffer.wrap(out);
		int coded = 0;
		int length = 0;
		while (i <= inLen || remLen != 0)
		{
			if (remLen != 0)
			{
				coded = remainder;
				remainder = 0;
				length = remLen;
				remLen = 0;
			}
			else
			{
				int code = 0;
				if (i < inLen)
				{
					int temp = in[i++] & 0xff;
					int temp2 = 0;
					if (i < inLen)
					{
						temp2 = in[i] & 0xff;
						int temp3 = codeExtended[temp][temp2];
						if (temp3 != 0)
						{
							int temp4 = 0;
							if (i + 1 < inLen)
							{
								temp4 = in[i + 1] & 0xff;
								int temp5 = codeExtended2[temp3 - 256][temp4];
								if (temp5 != 0)
								{
									int temp6 = 0;
									if (i + 2 < inLen)
									{
										temp6 = in[i + 2] & 0xff;
										int temp7 = codeExtended3[temp5 - 256][temp6];
										if (temp7 != 0)
										{
											i += 3;
											code = temp7;
										}
										else
										{
											i += 2;
											code = temp5;
										}
									}
									else
									{
										i += 2;
										code = temp5;
									}
								}
								else
								{
									i++;
									code = temp3;
								}
							}
							else
							{
								i++;
								code = temp3;
							}
						}
						else
						{
							code = temp;
						}
					}
					else
					{
						code = temp;
					}
				}
				else
				{
					i++;
				}
				coded = encode[code];
				length = encodeLength[code];
			}

			if (x - length >= 0)
			{
				value |= (coded << (x - length));
				x -= length;
			}
			else
			{
				value |= (coded >>> (length - x));
				remLen = length - x;
				remainder = coded & masks[remLen];
				x = 0;
			}

			if (x == 0)
			{
				bb.putInt(value);
				retval += 4;
				x = 32;
				value = 0;
			}
		}

		if (x == 32)
		{
			return retval;
		}

		if (x >= 24)
		{
			bb.put((byte)((value >>> 24)));
			retval++;
		}
		else if (x >= 16)
		{
			bb.putShort((short)((value >>> 16)));
			retval += 2;
		}
		else if (x >= 8)
		{
			bb.putShort((short)((value >>> 16)));
			bb.put((byte)((value >>> 8)));
			retval += 3;
		}
		else
		{
			bb.putInt(value);
			retval += 4;
		}

		return retval;
	}

	private static int decompress(byte[] in, int inLen, byte[] out) throws Exception
	{
		int i = 0;
		int o = 0;
		ByteBuffer bb = ByteBuffer.wrap(in);
		int remainder = 0;
		int remLen = 0;
		Code code = null;
		int value = 0;

		while (true)
		{
			int shift = 0;
			int shiftLen = 0;

			if (remLen >= 24)
			{
				shiftLen = remLen - 24;
				shift = remainder & masks[shiftLen];
				remainder = (remainder >>> (remLen - 24));
				value = 0;
			}
			else
			{
				final int free = 24 - remLen;
				value = (remainder << (free));
				int free2 = (inLen - i) << 3;

				if (free2 > free)
				{
					free2 = free;
				}

				if (free2 > 16)
				{
					shift = ((bb.getShort(i) & 0xffff) << 8) | (bb.get(i + 2) & 0xff);
					i += 3;
					remainder = (shift >>> remLen);
					shiftLen = remLen;
					shift = shift & masks[shiftLen];
				}
				else if (free2 > 8)
				{
					remainder = bb.getShort(i) & 0xffff;
					i += 2;
					if (free >= 16)
					{
						remainder = (remainder << (8 - remLen));
					}
					else
					{
						shiftLen = 16 - free;
						shift = remainder & masks[shiftLen];
						remainder = (remainder >>> shiftLen);
					}
				}
				else if (free2 > 0)
				{
					remainder = bb.get(i++) & 0xff;
					if (free >= 8)
					{
						remainder = (remainder << (16 - remLen));
					}
					else
					{
						shiftLen = 8 - free;
						shift = remainder & masks[shiftLen];
						remainder = (remainder >>> shiftLen);
					}
				}
				else
				{
					remainder = 0;
				}
			}

			value |= remainder;
			code = decode3[value];

			if (i < inLen)
			{
				System.arraycopy(code.bytes, 0, out, o, code.bytes.length);
				o += code.bytes.length;
			}
			else
			{
				for (byte b : code.bytes)
				{
					if (b == 0)
					{
						return o;
					}

					out[o++] = b;
				}
			}

			remLen = 24 - code.used;
			remainder = value & masks[remLen];
			remainder = ((remainder << shiftLen) | shift);
			remLen += shiftLen;
		}
	}

	private static Code readCode(ByteBuffer bb, InputStream in) throws Exception
	{
		int size = 0;
		byte[] bytes = null;
		short used = 0;
		if (bb.remaining() > 0)
		{
			size = bb.get();
		}
		else
		{
			in.read(bb.array());
			bb.position(0);
			size = bb.get();
		}

		bytes = new byte[size];
		if (bb.remaining() >= size)
		{
			bb.get(bytes);
		}
		else if (bb.remaining() == 0)
		{
			in.read(bb.array());
			bb.position(0);
			bb.get(bytes);
		}
		else
		{
			int i = 0;
			while (i < size)
			{
				if (bb.remaining() > 0)
				{
					bytes[i++] = bb.get();
				}
				else
				{
					in.read(bb.array());
					bb.position(0);
					bytes[i++] = bb.get();
				}
			}
		}

		if (bb.remaining() > 0)
		{
			used = bb.get();
		}
		else
		{
			in.read(bb.array());
			bb.position(0);
			used = bb.get();
		}

		Code code = new Code();
		code.bytes = bytes;
		code.used = used;
		return code;
	}

	private static void readDataFile() throws Exception
	{
		InputStream in = new FileInputStream("c:\\huffman2.dat");
		ByteBuffer bb = ByteBuffer.allocate(NUM_SYM * 4);
		in.read(bb.array());
		int i = 0;
		while (i < NUM_SYM)
		{
			encode[i] = bb.getInt();
			i++;
		}

		bb.position(0);
		in.read(bb.array());
		i = 0;
		while (i < NUM_SYM)
		{
			encodeLength[i] = bb.getInt();
			i++;
		}

		bb = ByteBuffer.allocate(8 * 1024 * 1024);
		in.read(bb.array());

		i = 0;
		while (i < 16777216)
		{
			decode3[i] = readCode(bb, in);
			i++;
		}

		in.close();
	}

	private static void traverse(HuffmanNode node, TreeMap<Integer, String> codes, String str)
	{
		if (node.left == null && node.right == null)
		{
			codes.put(node.bytecode, str);
		}
		else
		{
			if (node.left != null)
			{
				traverse(node.left, codes, str + "0");
			}

			if (node.right != null)
			{
				traverse(node.right, codes, str + "1");
			}
		}
	}

	private static int writeCode(Code code, ByteBuffer bb)
	{
		byte size = (byte)code.bytes.length;
		int retval = 0;
		if (bb.remaining() > 0)
		{
			bb.put(size);
			retval++;
		}
		else
		{
			return -1;
		}

		for (byte b : code.bytes)
		{
			if (bb.remaining() > 0)
			{
				bb.put(b);
				retval++;
			}
			else
			{
				return -1;
			}
		}

		if (bb.remaining() > 0)
		{
			bb.put((byte)code.used);
			retval++;
			return retval;
		}
		else
		{
			return -1;
		}
	}

	private static void writeDataFile() throws Exception
	{
		OutputStream out = new FileOutputStream("c:\\huffman2.dat");
		ByteBuffer bb = ByteBuffer.allocate(NUM_SYM * 4);
		int i = 0;
		while (i < NUM_SYM)
		{
			bb.putInt(encode[i]);
			i++;
		}

		out.write(bb.array());
		bb.position(0);
		i = 0;
		while (i < NUM_SYM)
		{
			bb.putInt(encodeLength[i]);
			i++;
		}

		out.write(bb.array());
		bb = ByteBuffer.allocate(8 * 1024 * 1024);
		i = 0;
		int pos = 0;
		while (i < 16777216)
		{
			int len = writeCode(decode3[i], bb);
			if (len != -1)
			{
				pos += len;
				i++;
			}
			else
			{
				out.write(bb.array(), 0, pos);
				bb.position(0);
				pos = 0;
			}
		}

		out.write(bb.array(), 0, pos);
		out.flush();
		out.close();
	}

	static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(Map<K, V> map)
	{
		SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2)
			{
				int res = e1.getValue().compareTo(e2.getValue());
				return res != 0 ? res : 1;
				// Special fix to preserve items with equal values
			}
		});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	private static class Code
	{
		public byte[] bytes;
		public short used;
	}

	private static class HuffmanNode
	{
		public int bytecode;
		public int freq;
		public HuffmanNode left;
		public HuffmanNode right;

		public HuffmanNode()
		{
			bytecode = -1;
			freq = 0;
		}

		public HuffmanNode(int bytecode, int freq)
		{
			this.bytecode = bytecode;
			this.freq = freq;
		}

		public void addLeft(HuffmanNode left)
		{
			this.left = left;
			this.freq += left.freq;
		}

		public void addRight(HuffmanNode right)
		{
			this.right = right;
			this.freq += right.freq;
		}
	}
}
