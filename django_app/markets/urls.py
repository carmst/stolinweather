from django.urls import path

from . import views


app_name = "markets"

urlpatterns = [
    path("", views.index, name="index"),
    path("marketplace/", views.marketplace, name="marketplace"),
    path("watchlist/", views.watchlist, name="watchlist"),
    path("history/", views.history, name="history"),
    path("marketplace/<str:ticker>/", views.market_detail, name="market_detail"),
]
