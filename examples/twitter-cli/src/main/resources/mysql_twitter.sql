SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `twitter`
--
CREATE DATABASE `twitter` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `twitter`;

-- --------------------------------------------------------

--
-- Table structure for table `followers`
--

CREATE TABLE `followers` (
  `follower` bigint(8) NOT NULL,
  `followed` bigint(8) NOT NULL,
  KEY `follower` (`follower`,`followed`),
  KEY `followed` (`followed`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `followers`
--

INSERT INTO `followers` (`follower`, `followed`) VALUES
(1388178689165000, 1388178455872000),
(1388178689165000, 1388178455872000);

-- --------------------------------------------------------

--
-- Table structure for table `mentions`
--

CREATE TABLE `mentions` (
  `tid` bigint(8) NOT NULL,
  `uid` bigint(8) NOT NULL,
  KEY `tid_2` (`tid`),
  KEY `uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `mentions`
--

INSERT INTO `mentions` (`tid`, `uid`) VALUES
(1388179288131000, 1388178689165000);

-- --------------------------------------------------------

--
-- Table structure for table `tweets`
--

CREATE TABLE `tweets` (
  `tid` bigint(8) NOT NULL,
  `author` bigint(8) NOT NULL,
  `message` text NOT NULL,
  `timestamp` bigint(8) NOT NULL,
  PRIMARY KEY (`tid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `tweets`
--

INSERT INTO `tweets` (`tid`, `author`, `message`, `timestamp`) VALUES
(1388178666361000, 1388178455872000, 'does this work?', 1388178666362000),
(1388178704213000, 1388178689165000, 'hi @jnelson', 1388178704214000),
(1388178956024000, 1388178455872000, 'hello@null', 1388178956024001),
(1388178962711000, 1388178455872000, 'hello @ashleah', 1388178962715000),
(1388179004450000, 1388178455872000, 'hello @ashleah love you', 1388179004450001),
(1388179070215000, 1388178455872000, 'hello @ashleah i really love you', 1388179070216000),
(1388179160500000, 1388178455872000, 'hi @ashleah i love you', 1388179160500001),
(1388179288131000, 1388178455872000, '@ashleah is really pretty', 1388179288131001);

-- --------------------------------------------------------

--
-- Table structure for table `users`
--

CREATE TABLE `users` (
  `uid` bigint(8) NOT NULL,
  `username` varchar(50) NOT NULL,
  `password` varchar(64) NOT NULL,
  `salt` bigint(8) NOT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `users`
--

INSERT INTO `users` (`uid`, `username`, `password`, `salt`) VALUES
(1388178455872000, 'jnelson', '264dd96dc9888ddbc96b1b4ec2fe3e71daf9ae3cda437432e24941a78c9c4009', 7780312038078664493),
(1388178689165000, 'ashleah', '6168df601595d4d040d914c75425359c35921934fa483bd5de6269554f8f783c', -7003299741269065862);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `followers`
--
ALTER TABLE `followers`
  ADD CONSTRAINT `followers_ibfk_2` FOREIGN KEY (`followed`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `followers_ibfk_1` FOREIGN KEY (`follower`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `mentions`
--
ALTER TABLE `mentions`
  ADD CONSTRAINT `mentions_ibfk_1` FOREIGN KEY (`tid`) REFERENCES `tweets` (`tid`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `mentions_ibfk_2` FOREIGN KEY (`uid`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE;
