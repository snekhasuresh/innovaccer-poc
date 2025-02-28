<?php
function enqueue_popular_bike_videos_css()
{
    wp_enqueue_style('popular-videos-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/popular-videos.css');
}

function popular_bike_videos($atts)
{
    enqueue_popular_bike_videos_css();
    // Arguments to get video posts
    $args = array(
        'post_type' => 'motocycle-video',
        'posts_per_page' => 5 // Adjust the number of posts as needed
    );
    $video_query = new WP_Query($args);

    ob_start();
?>
    <h2 style="font-size: 18px; font-weight: bold; margin-bottom: 16px;">Popular Videos</h2>
    <div class="popular-car-videos-container" style="list-style: none; padding: 0;">

        <?php if ($video_query->have_posts()): ?>
            <?php while ($video_query->have_posts()):
                $video_query->the_post();
                $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                $video_title = get_the_title();
                $views = get_post_meta(get_the_ID(), 'video_views', true); // Assume video views are stored as post meta
            ?>

                <div class="popular-car-video" style="display: flex; padding: 6px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                    <div class="video-thumbnail">
                        <img src="https://img.youtube.com/vi/<?php echo esc_attr($video_id); ?>/hqdefault.jpg"
                            style="cursor: pointer; width: 100px; height: 56px; border-radius: 5px; border: 1px solid #ccc;"
                            onclick="openModal('<?php echo esc_attr($video_id); ?>')" />
                    </div>
                    <div class="video-info" style="flex: 1;">
                        <h3 style="font-size: 14px; font-weight: bold; color: #333; margin: 0 0 4px; line-height: 18px; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; font-family:'Roboto'">
                            <?php echo esc_html($video_title); ?>
                        </h3>
                        <div class="video-views">
                            <?php echo esc_html($views); ?> views
                        </div>
                    </div>
                </div>

            <?php endwhile; ?>
        <?php endif; ?>

    </div>

    <!-- Modal Structure -->
    <div id="videoModal" class="modal">
        <div class="modal-content">
            <span class="close" onclick="closeModal()">&times;</span>
            <iframe id="videoFrame" width="100%" height="400px" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
        </div>
    </div>

    <script>
        // Open the modal and play the video
        function openModal(videoId) {
            var modal = document.getElementById('videoModal');
            var videoFrame = document.getElementById('videoFrame');
            videoFrame.src = "https://www.youtube.com/embed/" + videoId + "?autoplay=1&controls=1";
            modal.style.display = "flex";
        }

        // Close the modal and stop the video
        function closeModal() {
            var modal = document.getElementById('videoModal');
            var videoFrame = document.getElementById('videoFrame');
            videoFrame.src = "";
            modal.style.display = "none";
        }

        // Close modal if user clicks outside the modal content
        window.onclick = function(event) {
            var modal = document.getElementById('videoModal');
            if (event.target == modal) {
                closeModal();
            }
        }
    </script>

<?php
    wp_reset_postdata();
    return ob_get_clean();
}

add_shortcode('popular_bike_videos', 'popular_bike_videos');
