# Plugins Config
from xml.etree.cElementTree import parse as cet_parse
from os import path as os_path
from AutoTimerConfiguration import parseConfig, buildConfig

# Navigation (RecordTimer)
import NavigationInstance

# Timer
from ServiceReference import ServiceReference
from RecordTimer import RecordTimerEntry
from Components.TimerSanityCheck import TimerSanityCheck

# Timespan
from time import localtime, strftime, time, mktime
from datetime import timedelta, date

# EPGCache & Event
from enigma import eEPGCache, eServiceReference, eServiceCenter, iServiceInformation

# Enigma2 Config
from Components.config import config

# AutoTimer Component
from AutoTimerComponent import preferredAutoTimerComponent

XML_CONFIG = "/etc/enigma2/autotimer.xml"

def getTimeDiff(timer, begin, end):
	if begin <= timer.begin <= end:
		return end - timer.begin
	elif timer.begin <= begin <= timer.end:
		return timer.end - begin
	return 0

typeMap = {
	"exact": eEPGCache.EXAKT_TITLE_SEARCH,
	"partial": eEPGCache.PARTIAL_TITLE_SEARCH
}

caseMap = {
	"sensitive": eEPGCache.CASE_CHECK,
	"insensitive": eEPGCache.NO_CASE_CHECK
}

class AutoTimerNormalizedEvent:
	"""Class for event normalization
	
	This class is used to represent timers, movies and epg events.
	All objects of this class can easily be compared using the equal operator.
	"""
	
	def __init__(self, name, shortDescription, extendedDescription):
		"""Constructor
		
		Initialize the attributes of the object. 
		"""
		self.name = name
		self.shortDescription = shortDescription
		self.extendedDescription = extendedDescription
		
	def __str__(self):
		"""Convert object to string
		
		Useful for debugging purposes... 
		"""
		return str({
			'name': self.name,
			'shortDescription': self.shortDescription,
			'extendedDescription': self.extendedDescription
		})
	
	def __eq__(self, other):
		"""Compare two events
		
		If comparison can not be performed based on the name and the short description,
		we use the jaccard index of the extended descriptions.
		"""
		if not isinstance(other, AutoTimerNormalizedEvent):
			return false
		if self.name != other.name:
			return False
		if ('' == self.shortDescription or self.shortDescription == self.name) \
			and ('' == other.shortDescription or other.shortDescription == other.name):
			# Compare extended description
			return self.jaccardIndex(self.getWordsOfExtendedDescription(), other.getWordsOfExtendedDescription()) >= 0.9
		else:
			# Compare short description
			if self.shortDescription == other.shortDescription:
				return True 
		return False
	
	def getWordsOfExtendedDescription(self):
		"""Get a set with all words in the extended description.
		"""
		return set(self.extendedDescription.split(" "))

	def jaccardIndex(self, words1, words2):
		"""Calculate the jaccard index of to sets of words
		
		http://en.wikipedia.org/wiki/Jaccard_index
		"""
		return float(len(words1 & words2)) / float(len(words1 | words2))

class AutoTimer:
	"""Read and save xml configuration, query EPGCache"""

	def __init__(self):
		# Initialize
		self.timers = []
		self.configMtime = -1
		self.uniqueTimerId = 0
		self.defaultTimer = preferredAutoTimerComponent(
			0,		# Id
			"",		# Name
			"",		# Match
			True 	# Enabled
		)
		self.epgcache = eEPGCache.getInstance()
		# Create dict of all movies in all folders used by an autotimer to compare with recordings
		self.serviceHandler = eServiceCenter.getInstance()
		self.moviedict = {}
		self.normalizedTimers = {}
		print "[AutoTimer] init ..."

# Configuration

	def readXml(self):
		# Abort if no config found
		if not os_path.exists(XML_CONFIG):
			print "[AutoTimer] No configuration file present"
			return

		# Parse if mtime differs from whats saved
		mtime = os_path.getmtime(XML_CONFIG)
		if mtime == self.configMtime:
			print "[AutoTimer] No changes in configuration, won't parse"
			return

		# Save current mtime
		self.configMtime = mtime

		# Parse Config
		configuration = cet_parse(XML_CONFIG).getroot()

		# Empty out timers and reset Ids
		del self.timers[:]
		self.defaultTimer.clear(-1, True)

		parseConfig(
			configuration,
			self.timers,
			configuration.get("version"),
			0,
			self.defaultTimer
		)
		self.uniqueTimerId = len(self.timers)

	def getXml(self):
		return buildConfig(self.defaultTimer, self.timers, webif = True)

	def writeXml(self):
		file = open(XML_CONFIG, 'w')
		file.writelines(buildConfig(self.defaultTimer, self.timers))
		file.close()

# Manage List

	def add(self, timer):
		self.timers.append(timer)

	def getEnabledTimerList(self):
		return (x for x in self.timers if x.enabled)

	def getTimerList(self):
		return self.timers

	def getTupleTimerList(self):
		list = self.timers
		return [(x,) for x in list]

	def getSortedTupleTimerList(self):
		list = self.timers[:]
		list.sort()
		return [(x,) for x in list]

	def getUniqueId(self):
		self.uniqueTimerId += 1
		return self.uniqueTimerId

	def remove(self, uniqueId):
		idx = 0
		for timer in self.timers:
			if timer.id == uniqueId:
				self.timers.pop(idx)
				return
			idx += 1

	def set(self, timer):
		idx = 0
		for stimer in self.timers:
			if stimer == timer:
				self.timers[idx] = timer
				return
			idx += 1
		self.timers.append(timer)

	def normalizeEvent(self, event):
		"""Normalize an epg event
		
		Transform an epg event to an AutoTimerNormalizedEvent.
		"""
		return AutoTimerNormalizedEvent(
			event.getEventName(),
			event.getShortDescription(),
			event.getExtendedDescription()
		)
			
	def normalizeRecordTimer(self, timer):
		"""Normalize a record timer
		
		Transform a record timer to an AutoTimerNormalizedEvent.
		"""
		key = str(timer.service_ref) + str(timer.eit)
		if key in self.normalizedTimers:
			return self.normalizedTimers[key]
		
		extendedDescription = ""
		# Fetch extended description from EPG.
		# Will only work for timers in the future.
		if timer.eit is not None:
			event = self.epgcache.lookupEvent(['EX', ("%s" % timer.service_ref , 2, timer.eit)])
			if event and event[0][0] is not None:
				extendedDescription = event[0][0]
		
		result = AutoTimerNormalizedEvent(
			timer.name,
			timer.description,
			extendedDescription
		)
		
		self.normalizedTimers[key] = result
		return result

	def normalizeMovieRef(self, movieRef, serviceRef):
		"""Normalize a recorded movie
		
		Transform a movie to an AutoTimerNormalizedEvent.
		"""
		info = self.serviceHandler.info(movieRef)
		if info is None:
			return {}
		event = info.getEvent(movieRef)
		
		result = AutoTimerNormalizedEvent(
			info.getName(movieRef),
			info.getInfoString(serviceRef, iServiceInformation.sDescription),
			event and event.getExtendedDescription() or ""
		)
		
		return result

	def isEventInList(self, event, eventList, normalizeCallback = None):
		"""Check if a normalized event is similar to an event in a list
		
		If the list does not contain normalized events, the normalizeCallback must provide
		a function to use to normalize the events.
		"""
		# Parse sequences of entries
		for entry in eventList:
			if isinstance(eventList, dict):
				entry = eventList[entry]
			if isinstance(entry, list):
				if self.isEventInList(event, entry, normalizeCallback):
					return True
			else:
				if normalizeCallback is not None:
					entry = normalizeCallback(entry)
				if entry == event:
					return True
		return False
	
	def checkMovies(self, eventInfo, directory):
		"""Check if an event is similar to a recorded movie in a directory
		"""
		# Eventually create cache
		if directory and directory not in self.moviedict:
			serviceRef = eServiceReference("2:0:1:0:0:0:0:0:0:0:" + directory)
			movielist = self.serviceHandler.list(serviceRef)
			if movielist is None:
				print "[AutoTimer] Listing of movies in " + directory + " failed"
			else:
				self.moviedict.setdefault(directory, [])
				append = self.moviedict[directory].append
				while 1:
					movieRef = movielist.getNext()
					if not movieRef.valid():
						break
					if movieRef.flags & eServiceReference.mustDescent:
						continue
					append(self.normalizeMovieRef(movieRef, serviceRef))
				del append
				
		return self.isEventInList(eventInfo, self.moviedict.get(directory, []))

# Main function

	def parseEPG(self, simulateOnly = False):
		if NavigationInstance.instance is None:
			print "[AutoTimer] Navigation is not available, can't parse EPG"
			return (0, 0, 0, [], [])

		total = 0
		new = 0
		modified = 0
		timers = []
		conflicting = []

		# NOTE: the config option specifies "the next X days" which means today (== 1) + X
		delta = timedelta(days = config.plugins.autotimer.maxdaysinfuture.value + 1)
		evtLimit = mktime((date.today() + delta).timetuple())
		checkEvtLimit = delta.days > 1
		del delta

		self.readXml()

		# Save Recordings in a dict to speed things up a little
		# We include processed timers as we might search for duplicate descriptions
		recorddict = {}
		for rtimer in NavigationInstance.instance.RecordTimer.timer_list + NavigationInstance.instance.RecordTimer.processed_timers:
			if not rtimer.disabled:
				recorddict.setdefault(str(rtimer.service_ref), []).append(rtimer)

		# Iterate Timer
		for timer in self.getEnabledTimerList():
			# Precompute timer destination dir
			dest = timer.destination or config.usage.default_path.value

			# Workaround to allow search for umlauts if we know the encoding
			match = timer.match
			if timer.encoding != 'UTF-8':
				try:
					match = match.decode('UTF-8').encode(timer.encoding)
				except UnicodeDecodeError:
					pass

			# Search EPG, default to empty list
			ret = self.epgcache.search(('RI', 500, typeMap[timer.searchType], match, caseMap[timer.searchCase])) or ()

			for serviceref, eit in ret:
				eserviceref = eServiceReference(serviceref)

				evt = self.epgcache.lookupEventId(eserviceref, eit)
				if not evt:
					print "[AutoTimer] Could not create Event!"
					continue

				# Try to determine real service (we always choose the last one)
				n = evt.getNumOfLinkageServices()
				if n > 0:
					i = evt.getLinkageService(eserviceref, n-1)
					serviceref = i.toString()

				# Gather Information
				evtInfo = self.normalizeEvent(evt)
				evtBegin = begin = evt.getBeginTime()
				duration = evt.getDuration()
				evtEnd = end = begin + duration

				# If event starts in less than 60 seconds skip it
				if begin < time() + 60:
					print "[AutoTimer] Skipping an event because it starts in less than 60 seconds"
					continue

				# If maximum days in future is set then check time
				if checkEvtLimit:
					if begin > evtLimit:
						continue

				# Convert begin time
				timestamp = localtime(begin)

				# Update timer
				timer.update(begin, timestamp)

				# Check Duration, Timespan, Timeframe and Excludes
				if timer.checkServices(serviceref) \
					or timer.checkDuration(duration) \
					or timer.checkTimespan(timestamp) \
					or timer.checkTimeframe(begin) \
					or timer.checkFilter(
							evtInfo.name,
							evtInfo.shortDescription,
							evtInfo.extendedDescription,
							str(timestamp.tm_wday)
						):
					continue

				if timer.hasOffset():
					# Apply custom Offset
					begin, end = timer.applyOffset(begin, end)
				else:
					# Apply E2 Offset
					begin -= config.recording.margin_before.value * 60
					end += config.recording.margin_after.value * 60

				# Eventually change service to alternative
				if timer.overrideAlternatives:
					serviceref = timer.getAlternative(serviceref)

				total += 1

				# Append to timerlist and abort if simulating
				timers.append((evtInfo.name, begin, end, serviceref, timer.name))
				if simulateOnly:
					continue


                # Check for existing recordings in directory
				if timer.avoidDuplicateDescription == 3 and self.checkMovies(evtInfo, dest):
					continue

				# Initialize
				newEntry = None
				oldExists = False

				# Check for double Timers
				# We first check eit and if user wants us to guess event based on time
				# we try this as backup. The allowed diff should be configurable though.
				for rtimer in recorddict.get(serviceref, ()):
					if rtimer.eit == eit or config.plugins.autotimer.try_guessing.value and getTimeDiff(rtimer, begin, end) > ((duration/10)*8):
						oldExists = True

						# Abort if we don't want to modify timers or timer is repeated
						if config.plugins.autotimer.refresh.value == "none":
							print "[AutoTimer] Won't modify existing timer because no modification allowed"
							break
						if rtimer.repeated:
							print "[AutoTimer] Won't modify existing timer because repeated timer"
							break

						if hasattr(rtimer, "isAutoTimer"):
								rtimer.log(501, "[AutoTimer] AutoTimer %s modified this automatically generated timer." % (timer.name,))
						else:
							if config.plugins.autotimer.refresh.value != "all":
								print "[AutoTimer] Won't modify existing timer because it's no timer set by us"
								break

							rtimer.log(501, "[AutoTimer] Warning, AutoTimer %s messed with a timer which might not belong to it." % (timer.name,))

						newEntry = rtimer
						modified += 1

						# Modify values saved in timer
						newEntry.name = evtInfo.name
						newEntry.description = evtInfo.shortDescription
						newEntry.begin = int(begin)
						newEntry.end = int(end)
						newEntry.service_ref = ServiceReference(serviceref)

						break
					elif timer.avoidDuplicateDescription >= 1 and self.normalizeRecordTimer(rtimer) == evtInfo:
						oldExists = True
						print "[AutoTimer] We found a timer with same service and description, skipping event"
						break

				# We found no timer we want to edit
				if newEntry is None:
					# But there is a match
					if oldExists:
						continue

					# We want to search for possible doubles
					if timer.avoidDuplicateDescription >= 2 and self.isEventInList(evtInfo, recorddict, self.normalizeRecordTimer):
						print "[AutoTimer] We found a timer with same description, skipping event"
						continue

					if timer.checkCounter(timestamp):
						print "[AutoTimer] Not adding new timer because counter is depleted."
						continue

					newEntry = RecordTimerEntry(ServiceReference(serviceref), begin, end, evtInfo.name, evtInfo.shortDescription, eit)
					newEntry.log(500, "[AutoTimer] Adding new timer based on AutoTimer %s." % (timer.name,))

					# Mark this entry as AutoTimer (only AutoTimers will have this Attribute set)
					newEntry.isAutoTimer = True

				# Apply afterEvent
				if timer.hasAfterEvent():
					afterEvent = timer.getAfterEventTimespan(localtime(end))
					if afterEvent is None:
						afterEvent = timer.getAfterEvent()
					if afterEvent is not None:
						newEntry.afterEvent = afterEvent

				newEntry.dirname = timer.destination
				newEntry.justplay = timer.justplay
				newEntry.tags = timer.tags
				newEntry.vpsplugin_enabled = timer.vps_enabled
				newEntry.vpsplugin_overwrite = timer.vps_overwrite

				if oldExists:
					# XXX: this won't perform a sanity check, but do we actually want to do so?
					NavigationInstance.instance.RecordTimer.timeChanged(newEntry)
				else:
					conflicts = NavigationInstance.instance.RecordTimer.record(newEntry)
					if conflicts and config.plugins.autotimer.disabled_on_conflict.value:
						conflictString = ' / '.join(["%s (%s)" % (x.name, strftime("%Y%m%d %H%M", localtime(x.begin))) for x in conflicts])
						newEntry.log(503, "[AutoTimer] Timer disabled because of conflicts with %s." % (conflictString,))
						del conflictString
						newEntry.disabled = True
						# We might want to do the sanity check locally so we don't run it twice - but I consider this workaround a hack anyway
						conflicts = NavigationInstance.instance.RecordTimer.record(newEntry)
						conflicting.append((evtInfo.name, begin, end, serviceref, timer.name))
					if conflicts is None:
						timer.decrementCounter()
						new += 1
						recorddict.setdefault(serviceref, []).append(newEntry)
					else:
						conflicting.append((evtInfo.name, begin, end, serviceref, timer.name))

		return (total, new, modified, timers, conflicting)