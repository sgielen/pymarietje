from __future__ import with_statement
import threading
from joyce.comet import CometJoyceClient
from joyce.base import JoyceChannel
from mirte.threadPool import ThreadPool
from mirte.core import Module
import mirte
import mirte.main
import mirte.mirteFile

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080
DEFAULT_PATH = '/'
DEFAULT_LS_CHARSET = '1234567890qwertyuiopasdfghjklzxcvbnm '
DEFAULT_REQUESTER = 'marietje'

import os
import time
import socket
import logging
import cPickle as pickle
from cStringIO import StringIO
from lstree import SimpleCachingLSTree

class MarietjeException(Exception):
        pass
class AlreadyQueuedException(MarietjeException):
        pass
class AlreadyFetchingException(Exception):
        pass

class MarietjeClientChannel(JoyceChannel):
        def __init__(self, server, *args, **kwargs):
                super(MarietjeClientChannel, self).__init__(*args, **kwargs)
                self.s = server
                self.l = self.s.l
                self.partialMedia = []
                self._partialMedia = []
                self._partialMediaSize = None

        def handle_message(self, data):
                if data.get('type') == 'media_part':
                        with self.s.on_tracks_retrieved:
                                for media in data.get('part'):
                                        self._partialMedia.append(media)
                                if not self._partialMediaSize is None and self._partialMediaSize == len(self._partialMedia):
                                        self.partialMedia = self._partialMedia
                                        self._partialMediaSize = None
                                        self._partialMedia = []
                                        self.s.on_tracks_retrieved.notify()
                elif data.get('type') == 'media':
                        with self.s.on_tracks_retrieved:
                                self._partialMediaSize = data.get('count')
                                # Sometimes, media arrives before the media_parts
                                # In that case, wait for the rest of the media_parts
                                if len(self._partialMedia) == self._partialMediaSize:
                                        self.partialMedia = self._partialMedia
                                        self._partialMediaSize = None
                                        self._partialMedia = []
                                        self.s.on_tracks_retrieved.notify()
                elif data.get('type') == 'welcome':
                        pass
                elif data.get('type') == 'playing':
                        with self.s.on_playing_retrieved:
                                self.nowPlaying = data.get('playing')
                                self.s.on_playing_retrieved.notify()
                elif data.get('type') == 'requests':
                        with self.s.on_queue_retrieved:
                                self.requests = data.get('requests')
                                self.s.on_queue_retrieved.notify()
                else:
                        print "Data of type: ", data.get('type')
                        print "========================="
                        print data
                        print "========================="

class MarietjeClient(Module):
        """ Almost direct interface to the Marietje protocol """

        def __init__(self, settings, l):
                super(MarietjeClient, self).__init__(settings, l)
                def _channel_class(*args, **kwargs):
                        return MarietjeClientChannel(self, *args, **kwargs)
                self.channel = self.joyceClient.create_channel(
                        channel_class=_channel_class)

                self.on_queue_retrieved = threading.Condition()
                self.on_playing_retrieved = threading.Condition()
                self.on_tracks_retrieved = threading.Condition()

        def check_login(self, username):
                """ Checks whether <username> is allowed on marietje """
                self.channel.send_message({
                        'type': 'login',
                        'username': username,
                        'hash': None, # TODO
                })
                # TODO: wait for result
        
        def get_queue(self):
                """ Returns a list of ( artist, title, length, requestedBy )
                tuples. """
                with self.on_queue_retrieved:
                        self.channel.send_message({'type':'follow','which':['requests']})
                        self.on_queue_retrieved.wait()
                        requests = self.channel.requests
                        queue = []
                        for request in requests:
                                media = request.get('media')
                                requester = DEFAULT_REQUESTER
                                if not media.get('byKey') is None:
                                        requester = media.get('byKey')
                                queue.append((media.get('artist'), media.get('title'), media.get('length'), requester))
                        self.channel.send_message({'type':'unfollow','which':['requests']})
                return queue
        
        def get_playing(self):
                """ Return (id, timeStamp, length, time) with the
                    current playing's song <id> and <length>, the
                    current servers <time> and the starting time <timeStamp>
                    of the song """
                with self.on_playing_retrieved:
                        self.channel.send_message({'type':'follow','which':['playing']})
                        self.on_playing_retrieved.wait()
                        nowPlaying = self.channel.nowPlaying
                        media = nowPlaying.get('media')
                        nowPlaying = (
                                media.get('key'),
                                nowPlaying.get('endTime') - media.get('length'),
                                media.get('length'),
                                nowPlaying.get('serverTime'))
                        self.channel.send_message({'type':'unfollow','which':['playing']})
                return nowPlaying

        def list_tracks(self):
                """ Returns a list of
                     (trackId, artist, title, flag) """
                with self.on_tracks_retrieved:
                        self.channel.send_message({'type': 'list_media'})
                        self.on_tracks_retrieved.wait()
                        partialMedia = self.channel.partialMedia
                        res = []
                        for media in self.channel.partialMedia:
                                res.append((media.get('key'), media.get('artist'), media.get('title'), None))
                return res

        def request_track(self, trackId, user):
                """ Requests the song <trackId> under the username <user> """
                self.channel.send_message({
                        'type': 'request',
                        'mediaKey': trackId,
                })
                # TODO: wait until channel is done

        def upload_track(self, artist, title, user, size, f):
                """ Uploads <size> bytes of <f> as the track 
                    <artist> - <title> as <user> """
                raise NotImplemented()

class Marietje:
        """ A more convenient interface to Marietje.
            NOTE, even though there is a ton of threading.* goodness in here,
                  this class is not to be used by several threads at a time """
        def __init__(self, username, queueCb=None, songCb=None, playingCb=None,
                        host=DEFAULT_HOST, port=DEFAULT_PORT, path=DEFAULT_PATH,
                        charset=DEFAULT_LS_CHARSET):
                """ <xCb> is a callback for when x is fetched;
                    <charset> is used as charset for the livesearch look-up
                    tree. """
                self.mirte_manager = m = mirte.get_a_manager()
                mirte.mirteFile.load_mirteFile('joyce/comet', m)
                mirte.mirteFile.load_mirteFile('marietje', m)
                m.create_instance('joyceClient',
                        'cometJoyceClient',
                        {'host': host, 'port': port, 'path': path})
                self.client = m.create_instance('marietjeClient',
                        'marietjeClient', {'joyceClient':'joyceClient'})
                self.queueCb = queueCb
                self.songCb = songCb
                self.playingCb = playingCb
                self.songs_fetched = False
                self.queue_fetched = False
                self.playing_fetched = False
                self.songs_fetching = False
                self.queue_fetching = False
                self.playing_fetching = False
                self.songs_cond = threading.Condition()
                self.queue_cond = threading.Condition()
                self.playing_cond = threading.Condition()
                self.cs = charset
                self.cs_lut = set(charset)
                self.username = username
                self.l = logging.getLogger('Marietje')
        
        def _sanitize(self, txt):
                """ Prepares a str <txt> for live search """
                txt = txt.lower()
                ret = ''
                for c in txt:
                        if c in self.cs_lut:
                                ret += c
                return ret
        
        def _request_song_fetch(self):
                with self.songs_cond:
                        if self.songs_fetching:
                                raise AlreadyFetchingException
                        self.songs_fetching = True
        def _request_queue_fetch(self):
                with self.queue_cond:
                        if self.queue_fetching:
                                raise AlreadyFetchingException
                        self.queue_fetching = True
        def _request_playing_fetch(self):
                with self.playing_cond:
                        if self.playing_fetching:
                                raise AlreadyFetchingException
                        self.playing_fetching = True

        def start_fetch(self, fetchSongs=True,
                              fetchPlaying=True,
                              fetchQueue=True):
                try:
                        if fetchSongs:
                                self._request_song_fetch()
                except AlreadyFetchingException:
                        fetchSongs = False
                try:
                        if fetchPlaying:
                                self._request_playing_fetch()
                except AlreadyFetchingException:
                        fetchPlaying = False
                try:
                        if fetchQueue:
                                self._request_queue_fetch()
                except AlreadyFetchingException:
                        fetchQueue = False
                if fetchSongs: self.start_fetch_songs()
                if fetchQueue: self.start_fetch_queue()
                if fetchPlaying: self.start_fetch_playing()

        # These will be used if the annoying marietjed bug is fixed
        def start_fetch_songs(self):
                self.songs_thread = threading.Thread(target=self.run_fetch_songs)
                self.songs_thread.start()
        def start_fetch_queue(self):
                self.queue_thread = threading.Thread(target=self.run_fetch_queue)
                self.queue_thread.start()
        def start_fetch_playing(self):
                self.playing_thread = threading.Thread(target=self.run_fetch_playing)
                self.playing_thread.start()
        
        def run_fetch_songs(self):
                def entry_compare(x, y):
                        v = cmp(x[0], y[0])
                        return v if v != 0 else cmp(x[1], y[1])
                try:
                        starttime = time.time()
                        songs = dict()
                        for id, artist, title, flag in self.client.list_tracks():
                                songs[id] = (artist, title)
                        sLoadTime = time.time() - starttime
                        starttime = time.time()
                        entries = list()
                        for id, (artist, title) in songs.iteritems():
                                entries.append((self._sanitize(artist) + " " +
                                        self._sanitize(title), id))
                        sLut = SimpleCachingLSTree(entries, _cmp=entry_compare)
                        sLutGenTime = time.time() - starttime
                        with self.songs_cond:
                                self.songs = songs
                                self.sLoadTime = sLoadTime
                                self.sLutGenTime = sLutGenTime
                                self.sLut = sLut
                                self.songs_fetched = True
                except MarietjeException, e:
                        self.sException = e
                        self.l.exception("Marietje exception")
                except Exception:
                        self.l.exception("Uncaught exception")
                finally:
                        with self.songs_cond:
                                self.songs_fetching = False
                                self.songs_cond.notifyAll()
                        if not self.songCb is None:
                                self.songCb()
        
        def run_fetch_queue(self):
                try:
                        starttime = time.time()
                        queue = self.client.get_queue()
                        qLoadTime = time.time() - starttime
                        with self.queue_cond:
                                self.queue = queue
                                self.qLoadTime = qLoadTime
                                self.queue_fetched = True
                except MarietjeException, e:
                        self.qException = e
                        self.l.exception("Marietje exception")
                except Exception:
                        self.l.exception("Uncaught exception")
                finally:
                        with self.queue_cond:
                                self.queue_fetching = False
                                self.queue_cond.notifyAll()
                        if not self.queueCb is None:
                                self.queueCb()

        def run_fetch_playing(self):
                try:
                        starttime = time.time()
                        nowPlaying = self.client.get_playing()
                        pLoadTime = time.time() - starttime
                        playingRetreivedTime = starttime + 0.5 * pLoadTime
                        queueOffsetTime = nowPlaying[1] - (nowPlaying[3] - 
                                          playingRetreivedTime) + nowPlaying[2]
                        with self.playing_cond:
                                self.nowPlaying = nowPlaying
                                self.pLoadTime = pLoadTime
                                self.playingRetreivedTime = playingRetreivedTime
                                self.queueOffsetTime = queueOffsetTime
                                self.playing_fetched = True
                except MarietjeException, e:
                        self.pException = e
                        self.l.exception("Marietje exception")
                except Exception:
                        self.l.exception("Uncaught exception")
                finally:
                        with self.playing_cond:
                                self.playing_fetching = False
                                self.playing_cond.notifyAll()
                        if not self.playingCb is None:
                                self.playingCb()
        
        def cache_songs_to(self, f):
                """ Caches the songs and its look up structures to the given
                    file """
                with self.songs_cond:
                        if not self.songs_fetched:
                                raise RuntimeError, "songs haven't been fetched"
                        songs = self.songs
                        sLut = self.sLut
                
                sLut.prune()    
                pickle.dump((songs, sLut), f, pickle.HIGHEST_PROTOCOL)
        
        def songs_from_cache(self, f, abort_on_preempt=True):
                """ Fetches songs and its look up structure from a cache in
                    file created by <cache_songs_to>. Calls the callback
                    If after having loaded the cache, <songs_fetched> is set,
                    it'll abort if <abort_on_preempt>. """
                starttime = time.time()
                songs, sLut = pickle.load(f)
                sLoadTime = time.time() - starttime
                with self.songs_cond:
                        if abort_on_preempt and self.songs_fetched:
                                return
                        self.songs = songs
                        self.sLut = sLut
                        self.songs_fetched = True
                        self.sCacheLoadTime = sLoadTime
                if not self.songCb is None:
                        self.songCb(from_cache=True)
        
        def query(self, q):
                """ Performs a query for all songs that have <q> in their title
                    or artist.  Returns a list of ids """
                q = self._sanitize(q)
                # bit of a performance waster, but we don't want one track
                # several times in the results (when artist and title match)
                start = time.time()
                ret = tuple(self.sLut.query(q))
                self.l.info('query %s took %s' % (q, time.time() - start))
                return ret

        def request_track(self, track_id):
                """ Requests the track with id <track_id> """
                self.client.request_track(track_id, self.username)
        
        def upload_track(self, artist, title, size, f):
                """ Uploads a track in <f> with <size> to marietje as
                    <artist> - <title> """
                self.client.upload_track(artist, title, self.username, size, f)

if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)

        def print_queue():
                print "Queue received"
                print m.queue
        def print_songs():
                print len(m.songs), "songs received"
        def print_playing():
                print "Playing received"
                print m.nowPlaying

        m = Marietje("marietje", print_queue, print_songs, print_playing)
        m.start_fetch()
