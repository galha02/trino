select i.framerate,sessionid
from indigo.indigo_player_stat
lateral view explode(items) t as i
where day='2019-08-03'
and i.framerate=-2
limit 100