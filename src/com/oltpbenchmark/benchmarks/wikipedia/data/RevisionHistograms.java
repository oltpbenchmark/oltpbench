package com.oltpbenchmark.benchmarks.wikipedia.data;

import com.oltpbenchmark.util.Histogram;

public abstract class RevisionHistograms {

    /**
     * The length of the REV_COMMENT column
     */
    public static final Histogram<Integer> COMMENT_LENGTH = new Histogram<Integer>() {
        {
            this.put(0, 369676);
            this.put(1, 2349);
            this.put(2, 6499);
            this.put(3, 15283);
            this.put(4, 12587);
            this.put(5, 9478);
            this.put(6, 8950);
            this.put(7, 15733);
            this.put(8, 17814);
            this.put(9, 13030);
            this.put(10, 20114);
            this.put(11, 23820);
            this.put(12, 37709);
            this.put(13, 33519);
            this.put(14, 32407);
            this.put(15, 28522);
            this.put(16, 32136);
            this.put(17, 28346);
            this.put(18, 23220);
            this.put(19, 26814);
            this.put(20, 44981);
            this.put(21, 22521);
            this.put(22, 21283);
            this.put(23, 20584);
            this.put(24, 20573);
            this.put(25, 20744);
            this.put(26, 21000);
            this.put(27, 16677);
            this.put(28, 18865);
            this.put(29, 17696);
            this.put(30, 15825);
            this.put(31, 17675);
            this.put(32, 14153);
            this.put(33, 16588);
            this.put(34, 14771);
            this.put(35, 14556);
            this.put(36, 14078);
            this.put(37, 11642);
            this.put(38, 11149);
            this.put(39, 10627);
            this.put(40, 10310);
            this.put(41, 8686);
            this.put(42, 11333);
            this.put(43, 8712);
            this.put(44, 8030);
            this.put(45, 8106);
            this.put(46, 7814);
            this.put(47, 8776);
            this.put(48, 7439);
            this.put(49, 6503);
            this.put(50, 6323);
            this.put(51, 5874);
            this.put(52, 6472);
            this.put(53, 6024);
            this.put(54, 7171);
            this.put(55, 5338);
            this.put(56, 6252);
            this.put(57, 5208);
            this.put(58, 5260);
            this.put(59, 4390);
            this.put(60, 4723);
            this.put(61, 4467);
            this.put(62, 5498);
            this.put(63, 4129);
            this.put(64, 3989);
            this.put(65, 3594);
            this.put(66, 4851);
            this.put(67, 3588);
            this.put(68, 3825);
            this.put(69, 3705);
            this.put(70, 3212);
            this.put(71, 3124);
            this.put(72, 3775);
            this.put(73, 2853);
            this.put(74, 2969);
            this.put(75, 2785);
            this.put(76, 2796);
            this.put(77, 3964);
            this.put(78, 2514);
            this.put(79, 2493);
            this.put(80, 2955);
            this.put(81, 3000);
            this.put(82, 2916);
            this.put(83, 2223);
            this.put(84, 2247);
            this.put(85, 2489);
            this.put(86, 1892);
            this.put(87, 2705);
            this.put(88, 3226);
            this.put(89, 1994);
            this.put(90, 1852);
            this.put(91, 2070);
            this.put(92, 2066);
            this.put(93, 2371);
            this.put(94, 1961);
            this.put(95, 1803);
            this.put(96, 2305);
            this.put(97, 1812);
            this.put(98, 2145);
            this.put(99, 2203);
            this.put(100, 1837);
            this.put(101, 1799);
            this.put(102, 1726);
            this.put(103, 1730);
            this.put(104, 1890);
            this.put(105, 1688);
            this.put(106, 1478);
            this.put(107, 1659);
            this.put(108, 1396);
            this.put(109, 2088);
            this.put(110, 1772);
            this.put(111, 1158);
            this.put(112, 1234);
            this.put(113, 1756);
            this.put(114, 1417);
            this.put(115, 1173);
            this.put(116, 1480);
            this.put(117, 1447);
            this.put(118, 1640);
            this.put(119, 1910);
            this.put(120, 1851);
            this.put(121, 1699);
            this.put(122, 1994);
            this.put(123, 1464);
            this.put(124, 1504);
            this.put(125, 3023);
            this.put(126, 1779);
            this.put(127, 1572);
            this.put(128, 3420);
            this.put(129, 1463);
            this.put(130, 1637);
            this.put(131, 2772);
            this.put(132, 1751);
            this.put(133, 2315);
            this.put(134, 2190);
            this.put(135, 1690);
            this.put(136, 1889);
            this.put(137, 2412);
            this.put(138, 1839);
            this.put(139, 1665);
            this.put(140, 1512);
            this.put(141, 1267);
            this.put(142, 1543);
            this.put(143, 1057);
            this.put(144, 961);
            this.put(145, 1019);
            this.put(146, 835);
            this.put(147, 835);
            this.put(148, 914);
            this.put(149, 726);
            this.put(150, 688);
            this.put(151, 598);
            this.put(152, 786);
            this.put(153, 610);
            this.put(154, 702);
            this.put(155, 925);
            this.put(156, 739);
            this.put(157, 804);
            this.put(158, 578);
            this.put(159, 531);
            this.put(160, 555);
            this.put(161, 629);
            this.put(162, 532);
            this.put(163, 543);
            this.put(164, 548);
            this.put(165, 568);
            this.put(166, 616);
            this.put(167, 503);
            this.put(168, 596);
            this.put(169, 535);
            this.put(170, 614);
            this.put(171, 538);
            this.put(172, 479);
            this.put(173, 513);
            this.put(174, 613);
            this.put(175, 447);
            this.put(176, 488);
            this.put(177, 436);
            this.put(178, 497);
            this.put(179, 438);
            this.put(180, 490);
            this.put(181, 385);
            this.put(182, 440);
            this.put(183, 409);
            this.put(184, 509);
            this.put(185, 343);
            this.put(186, 483);
            this.put(187, 405);
            this.put(188, 401);
            this.put(189, 362);
            this.put(190, 462);
            this.put(191, 336);
            this.put(192, 433);
            this.put(193, 423);
            this.put(194, 345);
            this.put(195, 439);
            this.put(196, 393);
            this.put(197, 580);
            this.put(198, 581);
            this.put(199, 5151);
            this.put(200, 1201);
            this.put(201, 64);
            this.put(202, 56);
            this.put(203, 56);
            this.put(204, 43);
            this.put(205, 35);
            this.put(206, 41);
            this.put(207, 62);
            this.put(208, 66);
            this.put(209, 68);
            this.put(210, 82);
            this.put(211, 78);
            this.put(212, 50);
            this.put(213, 63);
            this.put(214, 59);
            this.put(215, 76);
            this.put(216, 76);
            this.put(217, 43);
            this.put(218, 38);
            this.put(219, 27);
            this.put(220, 36);
            this.put(221, 35);
            this.put(222, 37);
            this.put(223, 45);
            this.put(224, 32);
            this.put(225, 40);
            this.put(226, 30);
            this.put(227, 27);
            this.put(228, 16);
            this.put(229, 17);
            this.put(230, 18);
            this.put(231, 18);
            this.put(232, 11);
            this.put(233, 11);
            this.put(234, 21);
            this.put(235, 12);
            this.put(236, 11);
            this.put(237, 14);
            this.put(238, 10);
            this.put(239, 19);
            this.put(240, 11);
            this.put(241, 14);
            this.put(242, 8);
            this.put(243, 55);
            this.put(244, 19);
            this.put(245, 36);
            this.put(246, 35);
            this.put(247, 53);
            this.put(248, 96);
            this.put(249, 101);
            this.put(250, 173);
            this.put(251, 138);
            this.put(252, 110);
            this.put(253, 135);
            this.put(254, 34);
            this.put(255, 232);
        }
    };
    
    /**
     * 
     */
    public static final int REVISION_DELTA_SIZES[] = { 1000, 10000, 100000 };
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    public static final Histogram<Integer> REVISION_DELTAS[] = (Histogram<Integer>[])new Histogram[] {
        new Histogram<Integer>() {
            
        },
        new Histogram<Integer>() {
            
        },
        new Histogram<Integer>() {
            
        }
    };
    
    /**
     * The histogram of the REV_MINOR_EDIT column
     */
    public static final Histogram<Integer> MINOR_EDIT = new Histogram<Integer>() {
        {
            this.put(0, 1142822);
            this.put(1, 362171);
        }
    };
    
}
