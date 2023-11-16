import pandas as pd
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import RDF


def main():
    # Create an emprty graph
    g = Graph()
    g.parse("data/music_ontology.owl")

    # Loop through each triple in the graph (subj, pred, obj)
    for subj, pred, obj in g:
        # Check if there is at least one triple in the Graph
        if (subj, pred, obj) not in g:
            raise Exception("It better be!")

    # Print the number of "triples" in the Graph
    print(f"Graph g has {len(g)} statements.")

    base_url = "http://www.semanticweb.org/music_ontology"
    
    # Classes
    Track_class, Performer_class, Genre_class, Album_class = \
        [URIRef(f"{base_url}#{c}") for c in ["Track", "Performer", "Genre", "Album"]]

    # Object properties
    hasAssociatedGenre_op, hasGenre_op, partOf_op, performedBy_op = \
        [URIRef(f"{base_url}#{op}") for op in ["hasAssociatedGenre", "hasGenre", "partOf", "performedBy"]]
    
    # Data properties
    dp_names = [
        "acousticness", "country", "danceability", "performedBy", "duration", "energy", "instrumentalness",
        "label", "liveness", "loudness", "name", "popularity", "releaseYear", "speechiness", "tempo",
        "tracksTotal", "valence"
    ]

    df = pd.read_csv("data/final_df.csv")
    albums = pd.read_csv("data/albums.csv")
    artist = pd.read_csv("data/artist.csv")
    tracks = pd.read_csv("data/tracks.csv")
    performers = pd.read_csv("data/performers.csv")

    df.drop_duplicates(subset=['track_id'], inplace=True)
    data = df.merge(tracks, how='inner', left_on='spotify_id', right_on='id')
    data = data.merge(artist, how='inner', left_on='artist', right_on='artist_name')
    data = data.merge(albums, how='inner', left_on='album_id', right_on='album_id')
    data = data.merge(performers, how='inner', left_on='performer_id_x', right_on='performer_id')
    data['genre'] = data['genre'].replace(' ', '_', regex=True)

    df_mapping = {
        "name": "name_x",
        "duration": "duration_ms_x",
        "performedBy": "performer_id_x",
        "releaseYear": "year",
        "tracksTotal": "total_tracks"
    }

    for index, row in data.iterrows():
        track_individual = URIRef("http://www.semanticweb.org/music_ontology#track_"+row['track_id'])
        g.add((track_individual, RDF.type, Track_class))
        
        for dp_name in dp_names:
            if dp_name in ["country", "label"]:
                continue
            
            name_dp = URIRef(f"{base_url}#name")
            dp = URIRef(f"{base_url}#{dp_name}")

            df_name = dp_name
            if df_name not in data.columns:
                df_name = df_mapping[dp_name]

            g.add((track_individual, dp, Literal(row[df_name])))
        
            tracksTotal_dp = URIRef(f"{base_url}#tracksTotal")

            performer_individual = URIRef(f"{base_url}#performer_{row['performer_id_x']}")
            g.add((performer_individual, RDF.type, Performer_class))
            g.add((performer_individual, name_dp, Literal(row['artist'])))

            album_individual = URIRef(f"{base_url}#album_{row['album_id']}")
            g.add((album_individual, RDF.type, Album_class) )
            g.add((album_individual, tracksTotal_dp, Literal(row['total_tracks'])))

            genre_individual = URIRef(f"{base_url}#genre_{row['genre']}")
            g.add((genre_individual, RDF.type, Genre_class))
            g.add((genre_individual, name_dp, Literal(row['genre'])))

            # g.add((performer_individual, hasAssociatedGenre_op, genre_individual))
            g.add((track_individual, hasGenre_op, genre_individual))
            g.add((track_individual, partOf_op, album_individual))
            g.add((track_individual, performedBy_op, performer_individual))

    g.serialize("data/music_graph.ttl")
    g.close()


if __name__ == "__main__":
    main()
