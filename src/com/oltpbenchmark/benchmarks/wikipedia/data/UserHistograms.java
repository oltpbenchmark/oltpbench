package com.oltpbenchmark.benchmarks.wikipedia.data;

import com.oltpbenchmark.util.Histogram;

public abstract class UserHistograms {

    /**
     * The length of the USER_NAME column
     */
    public static final Histogram<Integer> NAME_LENGTH = new Histogram<Integer>() {
        {
            this.put(1, 29);
            this.put(2, 151);
            this.put(3, 1278);
            this.put(4, 2568);
            this.put(5, 5384);
            this.put(6, 9895);
            this.put(7, 12203);
            this.put(8, 13861);
            this.put(9, 11462);
            this.put(10, 10576);
            this.put(11, 8101);
            this.put(12, 6671);
            this.put(13, 4948);
            this.put(14, 3770);
            this.put(15, 2642);
            this.put(16, 1755);
            this.put(17, 1009);
            this.put(18, 691);
            this.put(19, 468);
            this.put(20, 372);
            this.put(21, 260);
            this.put(22, 185);
            this.put(23, 107);
            this.put(24, 75);
            this.put(25, 53);
            this.put(26, 52);
            this.put(27, 39);
            this.put(28, 29);
            this.put(29, 27);
            this.put(30, 27);
            this.put(31, 14);
            this.put(32, 12);
            this.put(33, 10);
            this.put(34, 9);
            this.put(35, 7);
            this.put(36, 6);
            this.put(37, 8);
            this.put(38, 6);
            this.put(39, 6);
            this.put(40, 4);
            this.put(41, 1);
            this.put(42, 2);
            this.put(44, 2);
            this.put(45, 1);
            this.put(47, 1);
            this.put(49, 1);
            this.put(50, 1);
            this.put(53, 3);
            this.put(54, 1);
            this.put(56, 2);
            this.put(60, 1);
            this.put(62, 1);
            this.put(63, 1);
        }
    };
    
    /**
     * The length of the USER_REAL_NAME column
     */
    public static final Histogram<Integer> REAL_NAME_LENGTH = new Histogram<Integer>() {
        {
            this.put(1, 29);
            this.put(2, 151);
            this.put(3, 1278);
            this.put(4, 2568);
            this.put(5, 5384);
            this.put(6, 9895);
            this.put(7, 12203);
            this.put(8, 13861);
            this.put(9, 11462);
            this.put(10, 10576);
            this.put(11, 8101);
            this.put(12, 6671);
            this.put(13, 4948);
            this.put(14, 3770);
            this.put(15, 2642);
            this.put(16, 1755);
            this.put(17, 1009);
            this.put(18, 691);
            this.put(19, 468);
            this.put(20, 372);
            this.put(21, 260);
            this.put(22, 185);
            this.put(23, 107);
            this.put(24, 75);
            this.put(25, 53);
            this.put(26, 52);
            this.put(27, 39);
            this.put(28, 29);
            this.put(29, 27);
            this.put(30, 27);
            this.put(31, 14);
            this.put(32, 12);
            this.put(33, 10);
            this.put(34, 9);
            this.put(35, 7);
            this.put(36, 6);
            this.put(37, 8);
            this.put(38, 6);
            this.put(39, 6);
            this.put(40, 4);
            this.put(41, 1);
            this.put(42, 2);
            this.put(44, 2);
            this.put(45, 1);
            this.put(47, 1);
            this.put(49, 1);
            this.put(50, 1);
            this.put(53, 3);
            this.put(54, 1);
            this.put(56, 2);
            this.put(60, 1);
            this.put(62, 1);
            this.put(63, 1);
        }
    };
    
    /**
     * The histogram of the number of users per revision updates 
     */
    public static final Histogram<Integer> REVISION_COUNT = new Histogram<Integer>() {
        {
            this.put(1, 41764);
            this.put(2, 16092);
            this.put(3, 8401);
            this.put(4, 5159);
            this.put(5, 3562);
            this.put(6, 2673);
            this.put(7, 2115);
            this.put(8, 1514);
            this.put(9, 1388);
            this.put(10, 1144);
            this.put(11, 961);
            this.put(12, 830);
            this.put(13, 728);
            this.put(14, 668);
            this.put(15, 547);
            this.put(16, 516);
            this.put(17, 477);
            this.put(18, 427);
            this.put(19, 392);
            this.put(20, 364);
            this.put(21, 328);
            this.put(22, 306);
            this.put(23, 278);
            this.put(24, 259);
            this.put(25, 273);
            this.put(26, 236);
            this.put(27, 223);
            this.put(28, 212);
            this.put(29, 207);
            this.put(30, 199);
            this.put(31, 181);
            this.put(32, 161);
            this.put(33, 172);
            this.put(34, 147);
            this.put(35, 148);
            this.put(36, 144);
            this.put(37, 131);
            this.put(38, 121);
            this.put(39, 118);
            this.put(40, 137);
            this.put(41, 108);
            this.put(42, 106);
            this.put(43, 101);
            this.put(44, 87);
            this.put(45, 102);
            this.put(46, 99);
            this.put(47, 82);
            this.put(48, 83);
            this.put(49, 88);
            this.put(50, 77);
            this.put(51, 68);
            this.put(52, 77);
            this.put(53, 63);
            this.put(54, 65);
            this.put(55, 72);
            this.put(56, 73);
            this.put(57, 58);
            this.put(58, 54);
            this.put(59, 47);
            this.put(60, 59);
            this.put(61, 53);
            this.put(62, 57);
            this.put(63, 59);
            this.put(64, 57);
            this.put(65, 53);
            this.put(66, 40);
            this.put(67, 44);
            this.put(68, 49);
            this.put(69, 38);
            this.put(70, 42);
            this.put(71, 53);
            this.put(72, 50);
            this.put(73, 40);
            this.put(74, 44);
            this.put(75, 50);
            this.put(76, 46);
            this.put(77, 45);
            this.put(78, 32);
            this.put(79, 37);
            this.put(80, 30);
            this.put(81, 34);
            this.put(82, 24);
            this.put(83, 34);
            this.put(84, 27);
            this.put(85, 27);
            this.put(86, 37);
            this.put(87, 28);
            this.put(88, 23);
            this.put(89, 26);
            this.put(90, 27);
            this.put(91, 19);
            this.put(92, 30);
            this.put(93, 34);
            this.put(94, 31);
            this.put(95, 28);
            this.put(96, 26);
            this.put(97, 31);
            this.put(98, 23);
            this.put(99, 33);
            this.put(100, 30);
            this.put(101, 22);
            this.put(102, 31);
            this.put(103, 21);
            this.put(104, 29);
            this.put(105, 23);
            this.put(106, 26);
            this.put(107, 21);
            this.put(108, 28);
            this.put(109, 28);
            this.put(110, 17);
            this.put(111, 18);
            this.put(112, 18);
            this.put(113, 17);
            this.put(114, 12);
            this.put(115, 25);
            this.put(116, 17);
            this.put(117, 15);
            this.put(118, 20);
            this.put(119, 19);
            this.put(120, 20);
            this.put(121, 18);
            this.put(122, 19);
            this.put(123, 16);
            this.put(124, 17);
            this.put(125, 14);
            this.put(126, 12);
            this.put(127, 11);
            this.put(128, 12);
            this.put(129, 19);
            this.put(130, 15);
            this.put(131, 21);
            this.put(132, 6);
            this.put(133, 14);
            this.put(134, 13);
            this.put(135, 17);
            this.put(136, 12);
            this.put(137, 12);
            this.put(138, 12);
            this.put(139, 15);
            this.put(140, 13);
            this.put(141, 11);
            this.put(142, 11);
            this.put(143, 8);
            this.put(144, 7);
            this.put(145, 14);
            this.put(146, 12);
            this.put(147, 18);
            this.put(148, 13);
            this.put(149, 13);
            this.put(150, 12);
            this.put(151, 15);
            this.put(152, 13);
            this.put(153, 12);
            this.put(154, 5);
            this.put(155, 13);
            this.put(156, 10);
            this.put(157, 7);
            this.put(158, 5);
            this.put(159, 4);
            this.put(160, 14);
            this.put(161, 9);
            this.put(162, 6);
            this.put(163, 8);
            this.put(164, 4);
            this.put(165, 11);
            this.put(166, 8);
            this.put(167, 13);
            this.put(168, 11);
            this.put(169, 8);
            this.put(170, 11);
            this.put(171, 7);
            this.put(172, 10);
            this.put(173, 5);
            this.put(174, 4);
            this.put(175, 9);
            this.put(176, 8);
            this.put(177, 5);
            this.put(178, 8);
            this.put(179, 8);
            this.put(180, 6);
            this.put(181, 4);
            this.put(182, 10);
            this.put(183, 9);
            this.put(184, 10);
            this.put(185, 6);
            this.put(186, 4);
            this.put(187, 6);
            this.put(188, 8);
            this.put(189, 6);
            this.put(190, 10);
            this.put(191, 4);
            this.put(192, 5);
            this.put(193, 7);
            this.put(194, 8);
            this.put(195, 5);
            this.put(196, 9);
            this.put(197, 10);
            this.put(198, 8);
            this.put(199, 7);
            this.put(200, 5);
            this.put(201, 3);
            this.put(202, 14);
            this.put(203, 5);
            this.put(204, 13);
            this.put(205, 3);
            this.put(206, 6);
            this.put(207, 5);
            this.put(208, 8);
            this.put(209, 6);
            this.put(210, 6);
            this.put(211, 5);
            this.put(212, 3);
            this.put(213, 9);
            this.put(214, 4);
            this.put(215, 3);
            this.put(216, 5);
            this.put(217, 5);
            this.put(218, 5);
            this.put(219, 6);
            this.put(220, 3);
            this.put(221, 8);
            this.put(222, 6);
            this.put(223, 8);
            this.put(224, 5);
            this.put(225, 7);
            this.put(226, 3);
            this.put(227, 2);
            this.put(228, 7);
            this.put(229, 3);
            this.put(230, 3);
            this.put(231, 6);
            this.put(232, 6);
            this.put(233, 7);
            this.put(234, 6);
            this.put(235, 4);
            this.put(236, 1);
            this.put(237, 2);
            this.put(238, 8);
            this.put(239, 6);
            this.put(240, 7);
            this.put(241, 3);
            this.put(242, 6);
            this.put(243, 4);
            this.put(244, 5);
            this.put(245, 3);
            this.put(246, 5);
            this.put(247, 5);
            this.put(248, 3);
            this.put(249, 5);
            this.put(250, 5);
            this.put(251, 2);
            this.put(252, 2);
            this.put(253, 4);
            this.put(254, 3);
            this.put(255, 5);
            this.put(256, 3);
            this.put(257, 4);
            this.put(258, 3);
            this.put(259, 1);
            this.put(260, 2);
            this.put(261, 4);
            this.put(262, 3);
            this.put(263, 4);
            this.put(264, 4);
            this.put(265, 2);
            this.put(266, 1);
            this.put(267, 4);
            this.put(268, 4);
            this.put(269, 2);
            this.put(270, 6);
            this.put(271, 2);
            this.put(272, 1);
            this.put(273, 2);
            this.put(274, 2);
            this.put(275, 2);
            this.put(276, 4);
            this.put(277, 3);
            this.put(278, 4);
            this.put(279, 2);
            this.put(280, 5);
            this.put(281, 1);
            this.put(282, 5);
            this.put(284, 3);
            this.put(286, 3);
            this.put(287, 4);
            this.put(288, 2);
            this.put(289, 3);
            this.put(290, 4);
            this.put(291, 2);
            this.put(292, 5);
            this.put(293, 4);
            this.put(294, 3);
            this.put(295, 2);
            this.put(296, 4);
            this.put(297, 3);
            this.put(298, 2);
            this.put(299, 3);
            this.put(300, 2);
            this.put(301, 2);
            this.put(302, 1);
            this.put(304, 3);
            this.put(305, 1);
            this.put(306, 1);
            this.put(307, 1);
            this.put(308, 2);
            this.put(310, 3);
            this.put(311, 1);
            this.put(312, 5);
            this.put(313, 2);
            this.put(314, 1);
            this.put(315, 3);
            this.put(316, 1);
            this.put(317, 1);
            this.put(318, 1);
            this.put(319, 3);
            this.put(321, 1);
            this.put(322, 1);
            this.put(323, 1);
            this.put(324, 5);
            this.put(326, 2);
            this.put(327, 2);
            this.put(329, 2);
            this.put(330, 3);
            this.put(331, 2);
            this.put(332, 4);
            this.put(333, 3);
            this.put(334, 3);
            this.put(335, 2);
            this.put(336, 3);
            this.put(337, 2);
            this.put(339, 3);
            this.put(340, 3);
            this.put(341, 2);
            this.put(342, 2);
            this.put(344, 2);
            this.put(345, 1);
            this.put(346, 1);
            this.put(347, 1);
            this.put(349, 2);
            this.put(350, 3);
            this.put(351, 2);
            this.put(352, 2);
            this.put(353, 3);
            this.put(355, 2);
            this.put(356, 1);
            this.put(357, 1);
            this.put(358, 1);
            this.put(359, 4);
            this.put(360, 3);
            this.put(363, 2);
            this.put(364, 2);
            this.put(365, 2);
            this.put(366, 2);
            this.put(367, 2);
            this.put(370, 1);
            this.put(373, 2);
            this.put(374, 2);
            this.put(376, 1);
            this.put(377, 1);
            this.put(378, 1);
            this.put(379, 1);
            this.put(380, 2);
            this.put(383, 2);
            this.put(386, 4);
            this.put(387, 2);
            this.put(388, 3);
            this.put(389, 1);
            this.put(390, 2);
            this.put(393, 1);
            this.put(395, 1);
            this.put(396, 1);
            this.put(397, 1);
            this.put(399, 2);
            this.put(401, 2);
            this.put(403, 3);
            this.put(404, 1);
            this.put(408, 3);
            this.put(412, 1);
            this.put(414, 3);
            this.put(415, 2);
            this.put(416, 1);
            this.put(417, 1);
            this.put(418, 1);
            this.put(419, 1);
            this.put(422, 1);
            this.put(423, 1);
            this.put(424, 2);
            this.put(425, 2);
            this.put(426, 2);
            this.put(428, 1);
            this.put(429, 2);
            this.put(430, 2);
            this.put(431, 1);
            this.put(433, 1);
            this.put(435, 2);
            this.put(437, 1);
            this.put(438, 2);
            this.put(439, 2);
            this.put(440, 2);
            this.put(443, 1);
            this.put(444, 1);
            this.put(445, 1);
            this.put(447, 1);
            this.put(449, 3);
            this.put(451, 1);
            this.put(452, 1);
            this.put(453, 2);
            this.put(456, 1);
            this.put(458, 1);
            this.put(459, 1);
            this.put(460, 1);
            this.put(461, 3);
            this.put(462, 2);
            this.put(464, 2);
            this.put(465, 1);
            this.put(467, 1);
            this.put(470, 1);
            this.put(479, 1);
            this.put(481, 1);
            this.put(483, 2);
            this.put(485, 1);
            this.put(486, 1);
            this.put(490, 2);
            this.put(492, 1);
            this.put(495, 1);
            this.put(497, 1);
            this.put(498, 1);
            this.put(502, 1);
            this.put(510, 1);
            this.put(515, 1);
            this.put(521, 1);
            this.put(527, 2);
            this.put(528, 2);
            this.put(530, 2);
            this.put(533, 1);
            this.put(536, 4);
            this.put(538, 2);
            this.put(540, 1);
            this.put(542, 1);
            this.put(543, 2);
            this.put(544, 1);
            this.put(547, 1);
            this.put(548, 1);
            this.put(555, 2);
            this.put(556, 1);
            this.put(559, 1);
            this.put(560, 1);
            this.put(561, 2);
            this.put(563, 1);
            this.put(564, 1);
            this.put(566, 1);
            this.put(570, 1);
            this.put(572, 1);
            this.put(573, 1);
            this.put(578, 1);
            this.put(592, 1);
            this.put(595, 1);
            this.put(599, 1);
            this.put(600, 2);
            this.put(604, 1);
            this.put(605, 1);
            this.put(607, 1);
            this.put(608, 1);
            this.put(612, 1);
            this.put(613, 1);
            this.put(617, 1);
            this.put(620, 1);
            this.put(626, 2);
            this.put(627, 1);
            this.put(629, 1);
            this.put(632, 1);
            this.put(633, 1);
            this.put(635, 1);
            this.put(636, 1);
            this.put(640, 1);
            this.put(641, 1);
            this.put(642, 1);
            this.put(644, 1);
            this.put(645, 1);
            this.put(651, 1);
            this.put(659, 1);
            this.put(671, 1);
            this.put(672, 1);
            this.put(675, 1);
            this.put(680, 1);
            this.put(681, 2);
            this.put(682, 1);
            this.put(684, 1);
            this.put(688, 1);
            this.put(692, 1);
            this.put(701, 1);
            this.put(702, 1);
            this.put(703, 2);
            this.put(718, 1);
            this.put(722, 1);
            this.put(728, 1);
            this.put(742, 2);
            this.put(758, 1);
            this.put(768, 1);
            this.put(773, 1);
            this.put(775, 1);
            this.put(777, 1);
            this.put(780, 1);
            this.put(786, 1);
            this.put(790, 1);
            this.put(794, 1);
            this.put(795, 1);
            this.put(796, 1);
            this.put(806, 1);
            this.put(807, 1);
            this.put(814, 1);
            this.put(834, 1);
            this.put(857, 1);
            this.put(858, 1);
            this.put(863, 1);
            this.put(873, 1);
            this.put(903, 1);
            this.put(931, 1);
            this.put(966, 1);
            this.put(982, 1);
            this.put(988, 2);
            this.put(992, 1);
            this.put(1033, 1);
            this.put(1038, 1);
            this.put(1055, 2);
            this.put(1077, 1);
            this.put(1086, 1);
            this.put(1095, 1);
            this.put(1098, 1);
            this.put(1137, 1);
            this.put(1143, 1);
            this.put(1151, 1);
            this.put(1180, 1);
            this.put(1207, 1);
            this.put(1240, 1);
            this.put(1264, 1);
            this.put(1282, 1);
            this.put(1292, 1);
            this.put(1299, 1);
            this.put(1355, 1);
            this.put(1433, 1);
            this.put(1468, 1);
            this.put(1482, 1);
            this.put(1531, 1);
            this.put(1580, 1);
            this.put(1614, 1);
            this.put(1668, 1);
            this.put(1828, 1);
            this.put(1885, 1);
            this.put(2135, 1);
            this.put(2235, 1);
            this.put(2250, 1);
            this.put(2442, 1);
            this.put(2460, 1);
            this.put(2566, 1);
            this.put(2714, 1);
            this.put(2932, 1);
            this.put(2954, 1);
            this.put(3275, 1);
            this.put(3407, 1);
            this.put(4581, 1);
            this.put(5466, 1);
            this.put(7454, 1);
            this.put(9742, 1);
        }
    };

}
