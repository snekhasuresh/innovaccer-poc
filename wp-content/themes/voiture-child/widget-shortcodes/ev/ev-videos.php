<?php
function enqueue_ev_videos_css()
{
    wp_enqueue_style('ev-videos-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-videos.css');
}

function ev_videos($atts)
{
    enqueue_ev_videos_css();

    $video_posts = get_latest_ev_videos_data();
    if (!$video_posts) {
        return;
    }
    ob_start();

    echo '<div class="popular-ev-head"><h2 class="wa-title-text">EV Videos</h2></div>';
    echo '<div class="ev-videos-container">';

    $post_count = 0; // Track the number of posts
    foreach ($video_posts as $video_post) {
        $video_id = $video_post['video_youtube_id'];
        $video_title = $video_post['title'];
        $video_url = sprintf('https://www.youtube.com/embed/%s', $video_id);

        // Add a class "hidden" to videos beyond the first 2
        $hidden_class = $post_count >= 2 ? 'hidden' : '';

        echo '<div class="ev-video ' . $hidden_class . '">';
        echo '<iframe src="' . $video_url . '" frameborder="0" allowfullscreen></iframe>';
        echo '<p class="ev-video-title">' . $video_title . '</p>';
        echo '</div>';

        $post_count++; // Increment post count
    }

    // "View More" button after first 2 videos
    if ($post_count > 2) {
        echo '<button id="view-more-btn"  style="background-color: white; border:none,color:#576b95; font-weignt:700;font-size:14px;"  class="button">
    View More <span class="icon">&#10095;</span> 
</button>
';
    }

    echo '</div>'; // Close video container

?>

    <script>
        jQuery(document).ready(function($) {
            $('#view-more-btn').on('click', function() {
                // Show all hidden videos
                $('.ev-video.hidden').removeClass('hidden');

                // Optionally hide the "View More" button after it's clicked
                $(this).hide();
            });
        });
    </script>

    <style>

    </style>

<?php
    wp_reset_postdata();

    wp_reset_query();

    return ob_get_clean();
}

add_shortcode('ev_videos', 'ev_videos');


add_action('wp_ajax_handle_view_more_videos', 'handle_view_more_videos');
add_action('wp_ajax_nopriv_handle_view_more_videos', 'handle_view_more_videos');
function handle_view_more_videos()
{
    if (isset($_POST['showMore'])) {
        ob_start();

        $showMore = intval($_POST['showMore']);
        if ($showMore > 0) {
            // show all posts

        } else {
            $showMore = 1;
        }
        $output = ob_get_clean();
        wp_send_json_success($output);
    } else {
        wp_send_json_error('Invalid request');
    }
}
