use rshtml::track_views_folder;

fn main() {
    // This function tells cargo to re-run the build script if
    // any file inside the configured views directory changes.
    track_views_folder();
}
