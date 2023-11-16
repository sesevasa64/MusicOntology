import pandas as pd
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import RDF
from tqdm import tqdm


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

    name_dp = URIRef(f"{base_url}#name")
    label_dp = URIRef(f"{base_url}#label")
    albumType_dp = URIRef(f"{base_url}#albumType")
    tracksTotal_dp = URIRef(f"{base_url}#tracksTotal")
    releaseYear_dp = URIRef(f"{base_url}#releaseYear")

    # Classes
    Track_class, Performer_class, Genre_class, Album_class = \
        [URIRef(f"{base_url}#{c}") for c in ["Track", "Performer", "Genre", "Album"]]

    # Object properties
    hasAssociatedGenre_op, hasGenre_op, partOf_op, performedBy_op = \
        [URIRef(f"{base_url}#{op}") for op in ["hasAssociatedGenre", "hasGenre", "partOf", "performedBy"]]
    
    # Data properties
    dp_names = [
        "acousticness", "country", "danceability", "performedBy", "duration", "energy", "instrumentalness",
        "label", "liveness", "loudness", "name", "popularity", "releaseYear", "speechiness", "tempo", "valence"
    ]

    df = pd.read_csv("data/final_df.csv")
    albums = pd.read_csv("data/albums_full.csv")
    artist = pd.read_csv("data/artist.csv")
    tracks = pd.read_csv("data/tracks.csv")
    performers = pd.read_csv("data/performers.csv")

    artist_id2name = dict(zip(artist["id"], artist["artist_name"]))

    def foo(artist_ids_raw: str):
        artist_names = []
        artist_ids = eval(artist_ids_raw)
        for artist_id in artist_ids:
            artist_names.append(artist_id2name[artist_id])
        return artist_names
    
    performers["artist_names"] = performers["artist_ids"].apply(foo)

    df.drop_duplicates(subset=['track_id'], inplace=True)
    data = df.merge(tracks, how='inner', left_on='spotify_id', right_on='id')
    data['genre'] = data['genre'].replace(' ', '_', regex=True)

    df_mapping = {
        "name": "name_x",
        "duration": "duration_ms_x",
        "performedBy": "performer_id",
        "releaseYear": "year",
    }

    for idx, row in tqdm(performers.iterrows(), total=len(performers)):
        performer_individual = URIRef(f"{base_url}#performer_{row['performer_id']}")
        g.add((performer_individual, RDF.type, Performer_class))
        g.add((performer_individual, name_dp, Literal(row['artist_names'][0])))

    for idx, row in tqdm(albums.iterrows(), total=len(albums)):
        album_individual = URIRef(f"{base_url}#album_{row['id']}")
        performer_individual = URIRef(f"{base_url}#performer_{row['performer_id']}")
        g.add((album_individual, RDF.type, Album_class))
        g.add((album_individual, name_dp, Literal(row['name'])))
        g.add((album_individual, tracksTotal_dp, Literal(row['total_tracks'])))
        g.add((album_individual, label_dp, Literal(row['label'])))
        g.add((album_individual, performedBy_op, performer_individual))
        g.add((album_individual, albumType_dp, Literal(row["album_type"])))
        if row["release_date_precision"] == "year":
            g.add((album_individual, releaseYear_dp, Literal(row['release_date'])))

    for genre in data["genre"].dropna().unique():
        genre_individual = URIRef(f"{base_url}#genre_{genre}")
        g.add((genre_individual, RDF.type, Genre_class))
        g.add((genre_individual, name_dp, Literal(genre)))

    for index, row in tqdm(data.iterrows(), total=len(data)):
        track_individual = URIRef(f"{base_url}#track_"+row['track_id'])
        g.add((track_individual, RDF.type, Track_class))
        
        performer_individual = URIRef(f"{base_url}#performer_{row['performer_id']}")
        album_individual = URIRef(f"{base_url}#album_{row['album_id']}")
        if not pd.isna(row['genre']):
            genre_individual = URIRef(f"{base_url}#genre_{row['genre']}")

        # g.add((performer_individual, hasAssociatedGenre_op, genre_individual))
        g.add((track_individual, hasGenre_op, genre_individual))
        g.add((track_individual, partOf_op, album_individual))
        g.add((track_individual, performedBy_op, performer_individual))

        for dp_name in dp_names:
            if dp_name in ["country", "label"]:
                continue
            
            tack_dp = URIRef(f"{base_url}#{dp_name}")

            df_name = dp_name
            if df_name not in data.columns:
                df_name = df_mapping[dp_name]

            g.add((track_individual, tack_dp, Literal(row[df_name])))

    g.serialize("data/music_graph.ttl")
    g.close()


if __name__ == "__main__":
    main()
