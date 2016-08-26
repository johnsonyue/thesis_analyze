import geoip2.database
import lookup
import qqwry
import os

class geoip_helper():
	def __init__(self):
		#bgp.
		self.lookup = lookup.lookup()
		
		#czdb.
		self.czdb_path = "qqwry.dat"
		if not os.path.exists(self.czdb_path):
			qqwry.update_db(self.czdb_path)
		self.qqwry = qqwry.QQWry(self.czdb_path)
		
		#mmdb.
		self.reader = geoip2.database.Reader('GeoLite2-City.mmdb')
	
	def query(self, ip):
		bgp = self.query_from_bgp(ip);
		mmdb = self.query_from_mmdb(ip);
		czdb = self.query_from_czdb(ip);
		return {"bgp":bgp, "mmdb":mmdb, "czdb":czdb}
	
	def query_from_bgp(self, ip):
		asn = self.lookup.get_asn_from_pfx(ip)
		if (asn != None):
			asn_cc = self.lookup.get_cc_from_asn(asn)
		else:
			asn = "*"
			asn_cc = "*"

		return {"asn":asn, "country":asn_cc}
	
	def query_from_mmdb(self, ip):
		try:
			response = self.reader.city(ip)
			return {"longitude":response.location.longitude, "latitude":response.location.latitude, "country":response.country.iso_code}
		except geoip2.errors.AddressNotFoundError:
			return {"longitude":"*", "latitude":"*", "country":"*"}
	
	def query_from_czdb(self, ip):
		try:
			country, area = self.qqwry.query(ip)
			return {"country":country, "area":area}
		except:
			return {"country":"*", "area":"*"}
