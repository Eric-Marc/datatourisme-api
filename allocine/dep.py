from allocineAPI.allocineAPI import allocineAPI
api = allocineAPI()
showtimes = api.get_showtime("P0186", "2025-12-09")
print(showtimes)