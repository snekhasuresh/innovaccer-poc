<?php
// Ajax handler for likes
add_action('wp_ajax_toggle_like', 'handle_toggle_like');
add_action('wp_ajax_nopriv_toggle_like', 'handle_toggle_like');

function handle_toggle_like()
{
    $post_name = $_POST['post_name'];
    $post_name = preg_replace('/-\d+$/', '', $post_name);
    // Get post by post_name
    $post = get_page_by_path($post_name, OBJECT, 'news');

    if (!$post) {
        wp_send_json_error('Post not found');
        return;
    }

    $post_id = $post->ID;
    $likes = get_post_meta($post_id, 'like_num', true) ?: 0;

    // Check if this is a like or unlike action
    if (isset($_COOKIE['liked_' . $post_id])) {
        // Unlike
        setcookie('liked_' . $post_id, '', time() - 3600, '/');
        update_post_meta($post_id, 'like_num', max(0, intval($likes) - 1));
        wp_send_json_success(['action' => 'unliked', 'likes' => $likes - 1]);
    } else {
        // Like
        setcookie('liked_' . $post_id, '1', time() + (86400 * 365), '/'); // Cookie expires in 1 year
        update_post_meta($post_id, 'like_num', intval($likes) + 1);
        wp_send_json_success(['action' => 'liked', 'likes' => $likes + 1]);
    }
}

function social_share_widget_shortcode()
{
    // Enqueue jQuery
    wp_enqueue_script('jquery');

    // Get current URL path
    $current_url = $_SERVER['REQUEST_URI'];
    $path_parts = explode('/', trim($current_url, '/'));
    $post_name = end($path_parts);

    // Get post and its likes
    $post_name = preg_replace('/-\d+$/', '', $post_name);
    $post = get_page_by_path($post_name, OBJECT, 'news');
    $likes = 0;
    $is_liked = false;

    if ($post) {
        $likes = get_post_meta($post->ID, 'like_num', true) ?: 0;
        $is_liked = isset($_COOKIE['liked_' . $post->ID]);
    }

    ob_start(); ?>
    <style>
        .social-share-widget {
            position: sticky;
            top: 100px !important;
            margin-top: 242px;
            margin-left: -51px !important;
            transform: translateY(-50%);
            display: flex;
            flex-direction: column;
            gap: 15px;
            z-index: 999;
        }

        .social-share-widget .share-item {
            width: 40px;
            height: 40px;
            background: white;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            cursor: pointer;
            transition: transform 0.2s;
            text-decoration: none;
        }

        .social-share-widget .share-item:hover {
            transform: scale(1.1);
        }

        .social-share-widget .share-item i {
            color: #333;
            font-size: 18px;
        }

        .social-share-widget .likes {
            position: relative;
        }

        .social-share-widget .likes-count {
            position: absolute;
            top: -8px;
            right: -8px;
            background: #ffb400;
            color: white;
            border-radius: 15px;
            padding: 2px 6px;
            font-size: 12px;
            font-weight: bold;
        }

        .social-share-widget .likes.active i {
            color: #ffb400;
        }

        @media screen and (max-width: 768px) {
            .social-share-widget {
                display: none;
            }
        }
    </style>

    <div class="social-share-widget">
        <div class="share-item likes <?php echo $is_liked ? 'active' : ''; ?>" data-post-name="<?php echo esc_attr($post_name); ?>">
            <i class="fas fa-thumbs-up" style="color: <?php echo $is_liked ? '#2ecc71' : '#333'; ?>;"></i>
            <span class="likes-count"><?php echo $likes; ?></span>
        </div>
        <!--         <div class="share-item">
            <i class="far fa-bookmark"></i>
        </div> -->
        <a href="#" class="share-item facebook-share">
            <i class="fab fa-facebook-f"></i>
        </a>
        <a href="#" class="share-item whatsapp-share">
            <i class="fab fa-whatsapp"></i>
        </a>
        <a href="#" class="share-item twitter-share">
            <i class="fab fa-twitter"></i>
        </a>
        <a href="#" class="share-item pinterest-share">
            <i class="fab fa-pinterest-p"></i>
        </a>
        <div class="share-item copy-link">
            <i class="fas fa-link"></i>
        </div>
    </div>

    <script>
        jQuery(document).ready(function($) {
            // Like button click handler
            $('.social-share-widget .likes').on('click', function() {
                var postName = $(this).data('post-name');
                postName = postName.replace(/-\d+$/, '');
                var $likeButton = $(this);

                $.ajax({
                    url: '<?php echo admin_url('admin-ajax.php'); ?>',
                    type: 'POST',
                    data: {
                        action: 'toggle_like',
                        post_name: postName
                    },
                    success: function(response) {
                        if (response.success) {
                            var $likesCount = $likeButton.find('.likes-count');
                            var $likeIcon = $likeButton.find('i');

                            $likesCount.text(response.data.likes);

                            if (response.data.action === 'liked') {
                                $likeButton.addClass('active');
                                $likeIcon.css('color', '#2ecc71');
                            } else {
                                $likeButton.removeClass('active');
                                $likeIcon.css('color', '#333');
                            }
                        }
                    }
                });
            });

            // Share functionality
            function getCurrentURL() {
                return window.location.href;
            }

            function getCurrentTitle() {
                return document.title;
            }

            $('.facebook-share').on('click', function(e) {
                e.preventDefault();
                var url = 'https://www.facebook.com/sharer/sharer.php?u=' + encodeURIComponent(getCurrentURL());
                window.open(url, '_blank', 'width=600,height=400');
            });

            $('.whatsapp-share').on('click', function(e) {
                e.preventDefault();
                var url = 'https://api.whatsapp.com/send?text=' + encodeURIComponent(getCurrentTitle() + ' ' + getCurrentURL());
                window.open(url, '_blank');
            });

            $('.twitter-share').on('click', function(e) {
                e.preventDefault();
                var url = 'https://twitter.com/intent/tweet?url=' + encodeURIComponent(getCurrentURL()) + '&text=' + encodeURIComponent(getCurrentTitle());
                window.open(url, '_blank', 'width=600,height=400');
            });

            $('.pinterest-share').on('click', function(e) {
                e.preventDefault();
                var url = 'https://pinterest.com/pin/create/button/?url=' + encodeURIComponent(getCurrentURL()) + '&description=' + encodeURIComponent(getCurrentTitle());
                window.open(url, '_blank', 'width=600,height=400');
            });

            $('.copy-link').on('click', function() {
                navigator.clipboard.writeText(getCurrentURL()).then(function() {
                    alert('Link copied to clipboard!');
                }).catch(function(err) {
                    console.error('Failed to copy text: ', err);
                });
            });
        });
    </script>

<?php
    return ob_get_clean();
}
add_shortcode('social_share_widget', 'social_share_widget_shortcode');
