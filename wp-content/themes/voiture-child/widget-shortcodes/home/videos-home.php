<?php

function videos_home_styles()
{
    wp_enqueue_style('videos-home-styles', get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/videos-home.css');
}

function youtube_videos_shortcode($atts)
{
    videos_home_styles();

    $video_ids = get_latest_videos_data('video');
    $video_ids = array_slice($video_ids, 0, 2);

    ob_start();
    if (!empty($video_ids)): ?>
        <div class="youtube-videos-container">
            <?php foreach ($video_ids as $video_id): ?>
                <div class="youtube-video" data-video-id="<?php echo esc_attr($video_id); ?>">
                    <div class="thumbnail-wrapper">
                        <img src="https://img.youtube.com/vi/<?php echo esc_attr($video_id); ?>/hqdefault.jpg"
                            alt="Video Thumbnail" class="youtube-thumbnail" loading="lazy">
                        <div class="play-button"></div>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        <script>
            document.addEventListener("DOMContentLoaded", function() {
                document.querySelectorAll(".youtube-video").forEach(video => {
                    video.addEventListener("click", function() {
                        let videoId = this.getAttribute("data-video-id");
                        this.innerHTML = `<iframe width="100%" height="244" 
                        src="https://www.youtube.com/embed/${videoId}?autoplay=1&controls=1&modestbranding=1&rel=0&loop=1&playlist=${videoId}&fs=0&iv_load_policy=3"
                        frameborder="0"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                        allowfullscreen>
                    </iframe>`;
                    });
                });
            });
        </script>
    <?php else: ?>
        <p>No videos available.</p>
    <?php endif; ?>
<?php
    return ob_get_clean();
}

add_shortcode('youtube_videos', 'youtube_videos_shortcode');
