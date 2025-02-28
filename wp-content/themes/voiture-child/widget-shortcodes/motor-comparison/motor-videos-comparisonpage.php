<?php
function enqueue_motor_videos_css()
{

    wp_enqueue_style('motor_videos_css', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-comparison/css/motor-videos.css');
}
function motor_comparison_videos_shortcode($atts)
{
    enqueue_motor_videos_css();
    $args = array(
        'post_type' => 'video',
        'posts_per_page' => 2
    );
    $video_query = new WP_Query($args);

    ob_start();
?>
    <div class="youtube-videos-container">
        <h2 class="wa-title-text">Cars Videos</h2>

        <?php if ($video_query->have_posts()): ?>
            <?php while ($video_query->have_posts()):
                $video_query->the_post();
                $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                $video_title = get_the_title(); // Get the video title
            ?>

                <div class="youtube-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px; cursor: pointer;" onclick="openModal('<?php echo esc_attr($video_id); ?>')">
                    <div class="video-overlay">
                        <img src="https://img.youtube.com/vi/<?php echo esc_attr($video_id); ?>/hqdefault.jpg" width="100%" height="120" style="border-radius: 5px;" />
                    </div>
                    <h3 style="color: black; text-align: start; margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em;">
                        <?php echo esc_html($video_title); ?>
                    </h3>
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
            videoFrame.src = ""; // Clear the video frame src to stop the video
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
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('motor_youtube_videos_comparisonpage', 'motor_comparison_videos_shortcode');
